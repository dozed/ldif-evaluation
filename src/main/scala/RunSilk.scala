
import de.fuberlin.wiwiss.silk.config._
import de.fuberlin.wiwiss.silk.config.RuntimeConfig
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.execution._
import de.fuberlin.wiwiss.silk.linkagerule.input._
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.output.Output
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality._
import de.fuberlin.wiwiss.silk.plugins.transformer.LowerCaseTransformer
import de.fuberlin.wiwiss.silk.plugins.transformer.phonetic.{MetaphoneTransformer, NysiisTransformer, SoundexTransformer}
import java.io.{PrintWriter, File}
import java.util.logging.Level
import ldif.local.{EvaluationType, Evaluation, SemPRecEvaluation}
import ldif.modules.silk.local.AlignmentApiWriter
import scala.xml.XML

/**
 * Created with IntelliJ IDEA.
 * User: stefan
 * Date: 25.09.13
 * Time: 14:54
 * To change this template use File | Settings | File Templates.
 */
trait RunSilk {

  implicit val prefixes = Prefixes.fromMap(Map(
    "rdf" -> "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs" -> "http://www.w3.org/2000/01/rdf-schema#",
    "owl" -> "http://www.w3.org/2002/07/owl#",
    "foaf" -> "http://xmlns.com/foaf/0.1/",
    "skos" -> "http://www.w3.org/2004/02/skos/core#",
    "dcterms" -> "http://purl.org/dc/terms/",
    "purl" -> "http://purl.org/dc/elements/1.1/",
    "dbpedia" -> "http://dbpedia.org/resource/",
    "openei" -> "http://en.openei.org/lod/resource/wiki/",
    "gn" -> "http://www.geonames.org/ontology#",
    "lgd" -> "http://linkedgeodata.org/",
    "lgdo" -> "http://linkedgeodata.org/ontology/",
    "faogeo" -> "http://aims.fao.org/aos/geopolitical.owl#",
    "reegle" -> "http://reegle.info/schema#",
    "glossary" -> "http://reegle.info/glossary/",
    "country" -> "http://reegle.info/countries/"))

  implicit val runtimeConfig = RuntimeConfig(executionMethod = ExecutionMethod(),
    blocking = Blocking(false),
    indexingOnly = false,
    useFileCache = false,
    reloadCache = true,
    partitionSize = 1000,
    numThreads = Runtime.getRuntime.availableProcessors(),
    generateLinksWithEntities = true,
    homeDir = System.getProperty("user.home") + "/.silk/",
    logLevel = Level.INFO)

  val base: String

  val sources: (Source, Source)

  val datasets: (Dataset, Dataset)

  def executeLinkConfig(sources: (Source, Source), linkSpec: LinkSpecification)(implicit config: RuntimeConfig) = {
    new GenerateLinksTask(
      sources = List(sources._1, sources._2),
      linkSpec = linkSpec,
      outputs = linkSpec.outputs,
      runtimeConfig = config
    ).apply()
  }

  case class EvaluationConfig(evalType: EvaluationType, alignmentRef: File, alignmentOut: File,
                              alignmentResults: File, rule: LinkageRule)

  def evaluate(conf: EvaluationConfig) = {
    val linkSpec = LinkSpecification(
      linkType = "http://www.w3.org/2002/07/owl#sameAs",
      datasets = datasets,
      rule = conf.rule,
      outputs = List(new Output("output", new AlignmentApiWriter(conf.alignmentOut))))

    executeLinkConfig(sources, linkSpec)

    Evaluation.eval(conf.evalType, conf.alignmentRef, conf.alignmentOut, conf.alignmentResults)

    val xml = XML.loadFile(conf.alignmentResults)
    val prec = (xml \ "output" \ "precision" text).toDouble
    val recall = (xml \ "output" \ "recall" text).toDouble
    (prec, recall)
  }

  def printFalsePositivesAndNegatives(conf: EvaluationConfig) = {

    val linkSpec = LinkSpecification(
      id = "geonamesReegle",
      linkType = "http://www.w3.org/2002/07/owl#sameAs",
      datasets = (Dataset("geonames", "a", SparqlRestriction.fromSparql("a", "?a rdf:type gn:Feature .")),
        Dataset("reegle", "b", SparqlRestriction.fromSparql("b", "?b rdf:type gn:Feature ."))),
      rule = conf.rule,
      outputs = List(new Output("output", new AlignmentApiWriter(conf.alignmentOut))))

    executeLinkConfig(sources, linkSpec)

    Evaluation.eval(conf.evalType, conf.alignmentRef, conf.alignmentOut, conf.alignmentResults)

    println(io.Source.fromFile(conf.alignmentResults).mkString)

    //val xml = XML.loadFile(conf.alignmentResults)
    //println(xml)
    //    val prec = (xml \ "output" \ "precision" text).toDouble
    //    val recall = (xml \ "output" \ "recall" text).toDouble
    //    (prec, recall)
  }

  // Input Transformations --------------------
  def transformInputWith(t: Transformer): Input => Input = {
    (in: Input) => TransformInput(transformer = t, inputs = Seq(in))
  }
  val lc = transformInputWith(LowerCaseTransformer())
  val soundex = transformInputWith(SoundexTransformer())
  val nysiis = transformInputWith(NysiisTransformer())
  val metaphone = transformInputWith(MetaphoneTransformer())
  val ml = transformInputWith(new Transformer {
    def apply(values: Seq[Set[String]]): Set[String] = {
      values.flatten.toList.sortBy(_.length).headOption.toSet
    }
  })
  def sw(stopwords: Seq[String]) = transformInputWith(new SimpleTransformer {
    def evaluate(value: String): String = {
      value.split(" ").filterNot(s => stopwords.contains(s.toLowerCase)).mkString(" ")
    }
  })


  // Link Rules & Evaluation ----------------------------------

  def evaluationConfig(rule: LinkageRule) = EvaluationConfig(SemPRecEvaluation,
    new File(f"$base/ldif-geo/alignment/align-named-ref.rdf"),
    File.createTempFile("alignment", ".tmp"),
    File.createTempFile("alignment-prec", ".tmp"),
    rule)

  def printToFile(file: File)(func: PrintWriter => Unit) {
    val pw = new PrintWriter(file)
    func.apply(pw)
    pw.close()
  }

  val metrics = List(
    "substring" -> SubStringDistance(),
    "jaro" -> JaroDistanceMetric(),
    "jaroWinkler" -> JaroWinklerDistance(),
    "levenshtein" -> LevenshteinMetric(),
    "equality" -> new RelaxedEqualityMetric(),
    "relaxed-equality" -> new RelaxedEqualityMetric(),
    "qgrams-2" -> QGramsMetric(q = 2),
    "qgrams-3" -> QGramsMetric(q = 3),
    "qgrams-4" -> QGramsMetric(q = 4),
    "qgrams-5" -> QGramsMetric(q = 5),
    "qgrams-6" -> QGramsMetric(q = 6),
    "qgrams-8" -> QGramsMetric(q = 8),
    "qgrams-10" -> QGramsMetric(q = 10)
  )

  type InputTransformation = Input => Input

}
