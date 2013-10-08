import com.hp.hpl.jena.query.{QuerySolution, ResultSet, QueryExecutionFactory}
import com.hp.hpl.jena.rdf.model.{ModelFactory, RDFNode, Model}
import com.hp.hpl.jena.reasoner.ReasonerRegistry
import de.fuberlin.wiwiss.silk.datasource.DataSource
import de.fuberlin.wiwiss.silk.entity.{Path, SparqlRestriction, EntityDescription}
import de.fuberlin.wiwiss.silk.util.sparql.{SparqlAggregatePathsCollector, EntityRetriever, Node, SparqlEndpoint}
import org.apache.jena.riot.{RDFDataMgr, Lang}
import collection.JavaConversions._

class JenaSparqlEndpoint(model: Model) extends SparqlEndpoint {
  /**
   * Executes a SPARQL SELECT query.
   */
  override def query(sparql: String, limit: Int): Traversable[Map[String, Node]] = {
    val qe = QueryExecutionFactory.create(sparql + " LIMIT " + limit, model)

    try {
      toSilkResults(qe.execSelect())
    }
    finally {
      qe.close()
    }
  }

  /**
   * Converts a Jena ARQ ResultSet to a Silk ResultSet.
   */
  private def toSilkResults(resultSet: ResultSet) = {
    val results =
      for (result <- resultSet) yield {
        toSilkBinding(result)
      }

    results.toList
  }

  /**
   * Converts a Jena ARQ QuerySolution to a Silk binding
   */
  private def toSilkBinding(querySolution: QuerySolution) = {
    val values =
      for (varName <- querySolution.varNames.toList;
           value <- Option(querySolution.get(varName))) yield {
        (varName, toSilkNode(value))
      }

    values.toMap
  }

  /**
   * Converts a Jena RDFNode to a Silk Node.
   */
  private def toSilkNode(node: RDFNode) = node match {
    case r: com.hp.hpl.jena.rdf.model.Resource if !r.isAnon => de.fuberlin.wiwiss.silk.util.sparql.Resource(r.getURI)
    case r: com.hp.hpl.jena.rdf.model.Resource => de.fuberlin.wiwiss.silk.util.sparql.BlankNode(r.getId.getLabelString)
    case l: com.hp.hpl.jena.rdf.model.Literal => de.fuberlin.wiwiss.silk.util.sparql.Literal(l.getString)
    case _ => throw new IllegalArgumentException("Unsupported Jena RDFNode type '" + node.getClass.getName + "' in Jena SPARQL results")
  }
}

case class NewFileDataSource(file: String, lang: Option[Lang] = None, inference: Boolean = false) extends DataSource {

  private lazy val model = {
    val m = lang match {
      case Some(lang) => RDFDataMgr.loadModel(file, lang)
      case None => RDFDataMgr.loadModel(file)
    }
    if (inference) {
      val reasoner = ReasonerRegistry.getRDFSReasoner
      ModelFactory.createInfModel(reasoner, m)
    } else m
  }

  private lazy val endpoint = new JenaSparqlEndpoint(model)

  override def retrieve(entityDesc: EntityDescription, entities: Seq[String]) = {
    EntityRetriever(endpoint).retrieve(entityDesc, entities)
  }

  override def retrievePaths(restrictions: SparqlRestriction, depth: Int, limit: Option[Int]): Traversable[(Path, Double)] = {
    SparqlAggregatePathsCollector(endpoint, restrictions, limit)
  }
}