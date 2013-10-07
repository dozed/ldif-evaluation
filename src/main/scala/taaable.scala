import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.{Entity, Path, SparqlRestriction}
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Comparison
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.SubStringDistance
import org.apache.jena.riot.Lang

object TaaableMatcher extends App with Evaluations {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/food.rdf", Some(Lang.RDFXML), true)),
    Source("dbpedia", NewFileDataSource(f"file:///$base/food.nt")))

  val datasets = (Dataset("taaable", "a", SparqlRestriction.fromSparql("a", "?a rdfs:subClassOf taaable:Category-3AFood .")),
    Dataset("dbpedia", "b", SparqlRestriction.fromSparql("a", "?a rdf:type owl:Thing .")))

  val inputs = (PathInput(path = Path.parse("?a/rdfs:label")) :: PathInput(path = Path.parse("?b/gn:countryCode")) :: Nil) map lc

  val linkSpec = LinkSpecification(
    linkType = "http://www.w3.org/2002/07/owl#sameAs",
    datasets = datasets,
    rule = LinkageRule(Comparison(
      threshold = 1.0,
      metric = SubStringDistance(),
      inputs = inputs
    )),
    outputs = List.empty)

  val entityDescs = linkSpec.entityDescriptions
  val entities = sources._1.retrieve(entityDescs._1)
  for (entity <- entities) {
    println(entity)
  }
  println(entities.size)

}
