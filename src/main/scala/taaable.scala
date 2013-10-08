import breeze.linalg.{Matrix, DenseMatrix}
import breeze.linalg._
import breeze.numerics._
import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{SimpleDistanceMeasure, DistanceMeasure, Comparison}
import de.fuberlin.wiwiss.silk.output.{Output, LinkWriter}
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io.PrintWriter
import org.apache.jena.riot.Lang
import scala.Some

object TaaableMatcher extends App with Evaluations2 with SparseDistanceMatrixIO {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  // in order to work with data from SPARQL endpoint:
  // val query2 = "?b dcterms:subject ?x . ?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> ."
  // Source("dbpedia", SparqlDataSource("http://lod.openlinksw.com/sparql")))

  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/taaable-food.rdf", Some(Lang.RDFXML), true)),
    Source("dbpedia", NewFileDataSource(f"file:///$base/dbpedia-foods.ttl", Some(Lang.TURTLE))))

  val query1 = "?a rdfs:subClassOf taaable:Category-3AFood ."

  val datasets = (Dataset("taaable", "a", SparqlRestriction.fromSparql("a", query1)),
    Dataset("dbpedia", "b", SparqlRestriction.empty))

  val inputs = (PathInput(path = Path.parse("?a/rdfs:label")) :: PathInput(path = Path.parse("?b/rdfs:label")) :: Nil) map lc

  val linkSpec = LinkSpecification(
    linkType = "http://www.w3.org/2002/07/owl#sameAs",
    datasets = datasets,
    rule = LinkageRule(Comparison(
      threshold = 0.2,
      metric = QGramsMetric(2),
      inputs = inputs
    )),
    outputs = List(new Output("output", new LinkWriter {
      def write(link: Link, predicateUri: String): Unit = {
        link.confidence match {
          case Some(1.0) => // println(f"= ${link.source} ${link.target}")
          case Some(c) => println(f"< ${c} ${link.source} ${link.target}")
          case None => println(f"No confidence value found for: $link")
        }
      }
    })))

  val measures = List(
//    "substring" -> SubStringDistance(),
    "qgrams2" -> QGramsMetric(q = 2),
    "jaro" -> JaroDistanceMetric(),
    "jaroWinkler" -> JaroWinklerDistance()
//    "levenshtein" -> LevenshteinMetric(),
//    "relaxedEquality" -> new RelaxedEqualityMetric()
  )

//  val entityDescs = linkSpec.entityDescriptions
//  val taaableEntities = entities(sources._1, entityDescs._1)
//  val dbpediaEntities = entities(sources._2, entityDescs._2)
//
//  val writer = writeSparseDistanceMatrix((taaableEntities, dbpediaEntities), 0.4)
//  measures foreach { case (l, d) => writer(new java.io.File(f"sim-$l.sparse"), d) }


//  val (m, n) = (2165, 29212)
//  val mat = readSparseDistanceMatrix(new java.io.File("sim-2-levenshtein.sparse"), m, n)
//  println(mat)
  // val mats = measures.toMap.keys map (l => (l, readMatrix(l)))

}

