import breeze.linalg.{Matrix, DenseMatrix}
import breeze.linalg._
import breeze.numerics._
import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.{EntityDescription, Link, Path, SparqlRestriction}
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Comparison
import de.fuberlin.wiwiss.silk.output.{Output, LinkWriter}
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io.PrintWriter
import org.apache.jena.riot.Lang

object TaaableMatcher extends App with Evaluations2 {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  // in order to work with data from SPARQL endpoint:

  //  CONSTRUCT {
  //    ?b <http://www.w3.org/2000/01/rdf-schema#label> ?v0
  //  } WHERE {
  //    ?b <http://purl.org/dc/terms/subject> ?x .
  //      ?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> .
  //      ?b <http://www.w3.org/2000/01/rdf-schema#label> ?v0 .
  //  }

  //  val query2 =
  //    """
  //      |?b dcterms:subject ?x .
  //      |?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> .
  //    """.stripMargin

  //  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/food.rdf", Some(Lang.RDFXML), true)),
  //    Source("dbpedia", SparqlDataSource("http://lod.openlinksw.com/sparql")))

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
    "substring" -> SubStringDistance(),
    "qgrams2" -> QGramsMetric(q = 2),
    "jaro" -> JaroDistanceMetric(),
    "jaroWinkler" -> JaroWinklerDistance(),
    "levenshtein" -> LevenshteinMetric(),
    "relaxedEquality" -> new RelaxedEqualityMetric()
  )

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

  def createMatrices = {
    val entityDescs = linkSpec.entityDescriptions
    val taaableEntities = entities(sources._1, entityDescs._1)
    val dbpediaEntities = entities(sources._2, entityDescs._2)

    var k = 0
    for {
      (label, measure) <- measures
    } {
      val pw = new PrintWriter(f"sim-$label.csv")

      for {
        ((e1, xs), i) <- taaableEntities.zipWithIndex.par
      } {
        val dists = for {
          ((e2, ys), j) <- dbpediaEntities.zipWithIndex
        } yield {
          k += 1
          if (k % 100000 == 0) println(k)

          val d = for {
            x <- xs
            y <- ys
          } yield {
            measure.evaluate(x, y)
          }

          val m = d.max

          if (m < 0.5) {
            Some((i, j, m))
          } else {
            None
          }
        }

        val l = dists.flatten
        pw.println(l.mkString(","))
      }

      pw.close()
    }
  }

  val (label, measure) = measures.head

//  (219,1166,0.4137254901960784),(219,22389,0.33333333333333337),(219,13942,0.27987220447284344),(219,13411,0.0)

  val m = 2165
  val n = 29212
  // val s = breeze.linalg.csvread(new java.io.File(f"sim-$label.csv"))
  // val lines = List("(219,1166,0.4137254901960784),(219,22389,0.33333333333333337),(219,13942,0.27987220447284344),(219,13411,0.0)")

  val sim = DenseMatrix.fill(m, n)(1.0)


  for {
    line <- io.Source.fromFile(f"sim-$label.csv").getLines
    if (line.size > 0)
    el <- line.split("""\),?""").map(_.substring(1).split(","))
  } {
    val (i, j, v) = (el(0).toInt, el(1).toInt, el(2).toDouble)
    sim(i, j) = v
  }

  println(sim(675, ::).toDenseVector.toArray.toList.filter(_ < 1.0))

}
