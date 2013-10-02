import de.fuberlin.wiwiss.silk.config.Dataset
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.SparqlRestriction

object TaaableMatcher extends App with Evaluations {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/food.nt")),
    Source("dbpedia", ???))

  val datasets = (Dataset("taaable", "a", SparqlRestriction.fromSparql("a", "?a rdf:type owl:Thing .")),
    Dataset("dbpedia", "b", _))



}
