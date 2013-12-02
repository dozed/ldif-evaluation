
import de.fuberlin.wiwiss.silk.config._
import de.fuberlin.wiwiss.silk.config.RuntimeConfig
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.{Entity, EntityDescription}
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
//import ldif.local.{Evaluation, SemPRecEvaluation}
//import ldif.modules.silk.local.AlignmentApiWriter
import scala.xml.XML

/**
 * Created with IntelliJ IDEA.
 * User: stefan
 * Date: 25.09.13
 * Time: 14:54
 * To change this template use File | Settings | File Templates.
 */
trait Evaluations {

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
    "country" -> "http://reegle.info/countries/",
    "taaable" -> "http://wikitaaable.loria.fr/index.php/Special:URIResolver/"))

  implicit val runtimeConfig = RuntimeConfig(executionMethod = ExecutionMethod(),
    blocking = Blocking(true),
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

  def entities(source: Source, desc: EntityDescription): Iterable[Entity] = {
    source.retrieve(desc).groupBy(_.uri).map { case (uri, xs) =>
      val e = xs.head
      val values = xs.map(_.values).flatten.toIndexedSeq
      new Entity(e.uri, values, e.desc)
    }
  }

  def printToFile(file: String)(func: PrintWriter => Unit) {
    val pw = new PrintWriter(file)
    func.apply(pw)
    pw.close()
  }

  def printToFile(file: File)(func: PrintWriter => Unit) {
    val pw = new PrintWriter(file)
    func.apply(pw)
    pw.close()
  }

  def evaluate(rule: LinkageRule)(implicit config: RuntimeConfig) = {
    // val evalType = SemPRecEvaluation
    val alignmentRef = new File(f"$base/alignment/align-named-ref.rdf")
    val alignmentOut = File.createTempFile("alignment", ".tmp")
    val alignmentResults = File.createTempFile("alignment-prec", ".tmp")

    val linkSpec = LinkSpecification(
      linkType = "http://www.w3.org/2002/07/owl#sameAs",
      datasets = datasets,
      rule = rule)
      // outputs = List(new Output("output", new AlignmentApiWriter(alignmentOut))))

    new GenerateLinksTask(
      sources = List(sources._1, sources._2),
      linkSpec = linkSpec,
      outputs = linkSpec.outputs,
      runtimeConfig = config
    ).apply()

    // Evaluation.eval(evalType, alignmentRef, alignmentOut, alignmentResults)

    val xml = XML.loadFile(alignmentResults)
    val prec = (xml \ "output" \ "precision" text).toDouble
    val recall = (xml \ "output" \ "recall" text).toDouble
    (prec, recall)
  }

  def runEvaluationWithVaryingThreshold(label: String, rulefunc: Double => LinkageRule) {
    println(f"evaluating $label")

    printToFile(new File(f"$base/evaluation/$label.txt")) {
      pw =>
        for (i <- 0 to 100) {
          val t = i / 100.0
          val (prec, recall) = evaluate(rulefunc(t))
          pw.println(f"${1.0 - t}, $prec, $recall")
          println(f"${1.0 - t}, $prec, $recall")
        }
    }
  }

  def runEvaluation(label: String, rule: LinkageRule) {
    println(f"evaluating $label")

    printToFile(new File(f"$base/evaluation/$label.txt")) {
      pw =>
        val (prec, recall) = evaluate(rule)
        pw.println(f"$prec, $recall")
        println(f"$prec, $recall")
    }
  }

  // Input Transformations --------------------
  type InputTransformation = Input => Input

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



}
