import com.hp.hpl.jena.query.{QuerySolution, ResultSet, QueryExecutionFactory}
import com.hp.hpl.jena.rdf.model.{ModelFactory, RDFNode, Model}
import com.hp.hpl.jena.reasoner.ReasonerRegistry
import de.fuberlin.wiwiss.silk.config.Prefixes
import de.fuberlin.wiwiss.silk.datasource.DataSource
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.linkagerule.input.Input
import de.fuberlin.wiwiss.silk.linkagerule.Operator
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Comparison, SimilarityOperator, DistanceMeasure}
import de.fuberlin.wiwiss.silk.util.{DPair, Identifier}
import de.fuberlin.wiwiss.silk.util.sparql.{SparqlAggregatePathsCollector, EntityRetriever, Node, SparqlEndpoint => SilkSparqlEndpoint}
import org.apache.jena.riot.{RDFDataMgr, Lang}
import collection.JavaConversions._
import scala.Some

class JenaSparqlEndpoint(model: Model) extends SilkSparqlEndpoint {
  /**
   * Executes a SPARQL SELECT query.
   */
  override def query(sparql: String, limit: Int) = {
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

  val model = {
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

class StructuralComparison(id: Identifier = Operator.generateId,
                           required: Boolean = false,
                           weight: Int = 1,
                           threshold: Double = 0.0,
                           indexing: Boolean = true,
                           metric: DistanceMeasure,
                           inputs: DPair[Input],
                           transformUri: String => String = identity)
  extends Comparison(id, required, weight, threshold, indexing, metric, inputs) {

  /**
   * Computes the similarity between two entities.
   *
   * @param entities The entities to be compared.
   * @param limit The confidence limit.
   *
   * @return The confidence as a value between -1.0 and 1.0.
   */
  override def apply(entities: DPair[Entity], limit: Double): Option[Double] = {

    val (uri1, uri2) = (transformUri(entities._1.uri), transformUri(entities._2.uri))

    val distance = metric(List(uri1), List(uri2), threshold * (1.0 - limit))

    if (distance == 0.0 && threshold == 0.0)
      Some(1.0)
    else if (distance <= 2.0 * threshold)
      Some(1.0 - distance / threshold)
    else if (!required)
      Some(-1.0)
    else
      None
  }

}