import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.execution.GenerateLinksTask
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Aggregation, Comparison}
import de.fuberlin.wiwiss.silk.output.{Output, LinkWriter}
import de.fuberlin.wiwiss.silk.plugins.aggegrator.AverageAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance

import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._

import java.io.FileInputStream

import org.apache.jena.riot.Lang

object TaaableEvaluation extends App with Evaluations {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  import GraphFactory._
  import Alg._

  // in order to work with DBpedia data:
  //
  // - SPARQL endpoint: broader* Category:Foods => timeouts
  //   val query2 = "?b dcterms:subject ?x . ?x <http://www.w3.org/2004/02/skos/core#broader>* <http://dbpedia.org/resource/Category:Foods> ."
  //   Source("dbpedia", SparqlDataSource("http://lod.openlinksw.com/sparql")))
  // - extract test dataset with name-based filter or blocking

  // setup Silk sources
  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/taaable-food.rdf", Some(Lang.RDFXML), true)),
    Source("dbpedia", NewFileDataSource(f"file:///$base/grain/dataset-grain-articles-categories-labels.nt", Some(Lang.NT))))

  val datasets = (Dataset("taaable", "a", SparqlRestriction.fromSparql("a", "?a rdfs:subClassOf taaable:Category-3AGrain .")),
    Dataset("dbpedia", "b", SparqlRestriction.empty))

  val inputs = (PathInput(path = Path.parse("?a/rdfs:label")) :: PathInput(path = Path.parse("?b/rdfs:label")) :: Nil) map lc

  // setup hierarchies
  val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
//  val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

  val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
//  val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

  val g = taaableHierarchy ++ dbpediaHierarchy ++
    merge("taaable:Food", "category:Food_and_drink") +
    ("category:Food_and_drink" ~> "common:Root" % 1) +
    ("taaable:Food" ~> "common:Root" % 1)

  // build Silk link specification
  val output = new Output("output", new LinkWriter {
    def write(link: Link, predicateUri: String): Unit = {
      link.confidence match {
        case Some(1.0) => println(f"= ${link.source} ${link.target}")
        case Some(c) => println(f"< ${c} ${link.source} ${link.target}")
        case None => println(f"No confidence value found for: $link")
      }
    }
  })

  val linkSpec = LinkSpecification(
    linkType = "http://www.w3.org/2002/07/owl#sameAs",
    datasets = datasets,
    rule = LinkageRule(Aggregation(
      required = false,
      weight = 1,
      operators = List(Comparison(
        threshold = 0.2,
        metric = JaroWinklerDistance(),
        inputs = inputs
      ), Comparison(
        threshold = 1.0,
        metric = WuPalmer(g, "common:Root"),
        inputs = inputs
      )),
      aggregator = AverageAggregator()
    )),
    outputs = List(output))


  // run link specification
  new GenerateLinksTask(
    sources = List(sources._1, sources._2),
    linkSpec = linkSpec,
    outputs = linkSpec.outputs,
    runtimeConfig = runtimeConfig
  ).apply()


}

