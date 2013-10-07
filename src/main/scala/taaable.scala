import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.{EntityDescription, Link, Path, SparqlRestriction}
import de.fuberlin.wiwiss.silk.execution.GenerateLinksTask
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Aggregation, Comparison}
import de.fuberlin.wiwiss.silk.output.{Output, LinkWriter}
import de.fuberlin.wiwiss.silk.plugins.aggegrator.AverageAggregator
import de.fuberlin.wiwiss.silk.plugins.datasource.SparqlDataSource
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import org.apache.jena.riot.Lang
import scala.Some
import scala.Some

object TaaableMatcher extends App with Evaluations {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  //  CONSTRUCT {
  //    ?b <http://www.w3.org/2000/01/rdf-schema#label> ?v0
  //  } WHERE {
  //    ?b <http://purl.org/dc/terms/subject> ?x .
  //      ?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> .
  //      ?b <http://www.w3.org/2000/01/rdf-schema#label> ?v0 .
  //  }

  //  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/food.rdf", Some(Lang.RDFXML), true)),
  //    Source("dbpedia", SparqlDataSource("http://lod.openlinksw.com/sparql")))

  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/food.rdf", Some(Lang.RDFXML), true)),
    Source("dbpedia", NewFileDataSource(f"file:///$base/dbpedia-foods.ttl", Some(Lang.TURTLE))))

  val query1 = "?a rdfs:subClassOf taaable:Category-3AFood ."

  //  val query2 =
  //    """
  //      |?b dcterms:subject ?x .
  //      |?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> .
  //    """.stripMargin

  val query2 = "?a a owl:Thing ."

  val datasets = (Dataset("taaable", "a", SparqlRestriction.fromSparql("a", query1)),
    Dataset("dbpedia", "b", SparqlRestriction.empty))

  val inputs = (PathInput(path = Path.parse("?a/rdfs:label")) :: PathInput(path = Path.parse("?b/rdfs:label")) :: Nil) map lc

  val linkSpec = LinkSpecification(
    linkType = "http://www.w3.org/2002/07/owl#sameAs",
    datasets = datasets,
    //    rule = LinkageRule(Aggregation(
    //      aggregator = AverageAggregator(),
    //      operators = Seq(Comparison(
    //        threshold = 0.2,
    //        metric = LevenshteinDistance(),
    //        inputs = inputs
    //      ), Comparison(
    //        threshold = 0.2,
    //        metric = SubStringDistance(),
    //        inputs = inputs
    //      )))),
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


//  new GenerateLinksTask(
//    sources = List(sources._1, sources._2),
//    linkSpec = linkSpec,
//    outputs = linkSpec.outputs,
//    runtimeConfig = runtimeConfig
//  ).apply()

  val metrics2 = List(
    "substring" -> SubStringDistance(),
    "jaro" -> JaroDistanceMetric(),
    "jaroWinkler" -> JaroWinklerDistance(),
    "levenshtein" -> LevenshteinMetric(),
    "equality" -> new RelaxedEqualityMetric(),
    "relaxed-equality" -> new RelaxedEqualityMetric(),
    "qgrams-2" -> QGramsMetric(q = 2)
  )

  println(QGramsMetric(2).evaluate("Chinese noodle", "Chinese noodles"))

  def entities(source: Source, desc: EntityDescription): Map[String, Traversable[String]] = {
    for {
      (k, xs) <- source.retrieve(desc).groupBy(_.uri)
    } yield {
      val ys = for {
        x <- xs
        v <- x.values
        v2 <- v
      } yield v2
      (k, ys)
    }
  }

  val entityDescs = linkSpec.entityDescriptions
  val dbpediaEntities = entities(sources._1, entityDescs._1)
  val taaableEntities = entities(sources._2, entityDescs._2)

  taaableEntities foreach println

  // Evaluation.eval(evalType, alignmentRef, alignmentOut, alignmentResults)

  //  val xml = XML.loadFile(alignmentResults)
  //  val prec = (xml \ "output" \ "precision" text).toDouble
  //  val recall = (xml \ "output" \ "recall" text).toDouble
  //  (prec, recall)


  //  val entityDescs = linkSpec.entityDescriptions
  //  val entities = sources._1.retrieve(entityDescs._1)
  //  for (entity <- entities) {
  //    println(entity)
  //  }
  //  println(entities.size)

}
