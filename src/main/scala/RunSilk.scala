
import com.hp.hpl.jena.query.{QueryExecutionFactory, ResultSet, QuerySolution}
import com.hp.hpl.jena.rdf.model.{Model, RDFNode}
import de.fuberlin.wiwiss.silk.cache.{MemoryEntityCache, FileEntityCache}
import de.fuberlin.wiwiss.silk.config._
import de.fuberlin.wiwiss.silk.config.RuntimeConfig
import de.fuberlin.wiwiss.silk.datasource.{DataSource, Source}
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.execution._
import de.fuberlin.wiwiss.silk.linkagerule.input._
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Comparison
import de.fuberlin.wiwiss.silk.output.Output
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.equality._
import de.fuberlin.wiwiss.silk.plugins.transformer._
import de.fuberlin.wiwiss.silk.plugins.transformer.phonetic.{NysiisTransformer, SoundexTransformer}
import de.fuberlin.wiwiss.silk.util.{DPair, CollectLogs}
import de.fuberlin.wiwiss.silk.util.sparql.{Node, SparqlEndpoint, SparqlAggregatePathsCollector, EntityRetriever}
import de.fuberlin.wiwiss.silk.util.task.ValueTask
import java.io.File
import java.util.logging.Level
import ldif.local.{SemPRecEvaluation, EvaluationType, Evaluation}
import ldif.modules.silk.local.AlignmentApiWriter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import scala.collection.JavaConversions._
import scala.Some
import scala.Some
import scala.xml.XML


private class JenaSparqlEndpoint(model: Model) extends SparqlEndpoint {
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

case class NewFileDataSource(file: String, lang: Option[Lang] = None) extends DataSource {

  private lazy val model = lang match {
    case Some(lang) => RDFDataMgr.loadModel(file, lang)
    case None => RDFDataMgr.loadModel(file)
  }

  private lazy val endpoint = new JenaSparqlEndpoint(model)

  override def retrieve(entityDesc: EntityDescription, entities: Seq[String]) = {
    EntityRetriever(endpoint).retrieve(entityDesc, entities)
  }

  override def retrievePaths(restrictions: SparqlRestriction, depth: Int, limit: Option[Int]): Traversable[(Path, Double)] = {
    SparqlAggregatePathsCollector(endpoint, restrictions, limit)
  }
}

//class GenerateLinksTask(sources: Traversable[Source],
//                        linkSpec: LinkSpecification,
//                        outputs: Traversable[Output] = Traversable.empty,
//                        runtimeConfig: RuntimeConfig = RuntimeConfig()) extends ValueTask[Seq[Link]](Seq.empty) {
//
//  override protected def execute() = {
//    value.update(Seq.empty)
//
//    //Retrieve sources
//    val sourcePair = linkSpec.datasets.map(_.sourceId).map(id => sources.find(_.id == id).get)
//
//    //Entity caches
//    val caches = createCaches()
//
//    //Create tasks
//    loadTask = new LoadTask(sourcePair, caches)
//    matchTask = new MatchTask(linkSpec.rule, caches, runtimeConfig)
//
//    //Load entities
//    if (runtimeConfig.reloadCache) {
//      loadTask.statusLogLevel = statusLogLevel
//      loadTask.progressLogLevel = progressLogLevel
//      loadTask.runInBackground()
//    }
//
//    //Execute matching
//    val links = executeSubValueTask(matchTask, 0.95)
//
//    //Filter links
//    val filterTask = new FilterTask(links, linkSpec.filter)
//    value.update(executeSubTask(filterTask))
//
//    //Output links
//    val outputTask = new OutputTask(value.get, linkSpec.linkType, outputs)
//    executeSubTask(outputTask)
//
//    //Return generated links
//    value.get
//  }
//
//  private def createCaches() = {
//    val indexFunction = (entity: Entity) => runtimeConfig.executionMethod.indexEntity(entity, linkSpec.rule)
//
//    if (runtimeConfig.useFileCache) {
//      val cacheDir = new File(runtimeConfig.homeDir + "/entityCache/" + linkSpec.id)
//
//      DPair(
//        source = new FileEntityCache(entityDescs.source, indexFunction, cacheDir + "/source/", runtimeConfig),
//        target = new FileEntityCache(entityDescs.target, indexFunction, cacheDir + "/target/", runtimeConfig)
//      )
//    } else {
//      DPair(
//        source = new MemoryEntityCache(entityDescs.source, indexFunction, runtimeConfig),
//        target = new MemoryEntityCache(entityDescs.target, indexFunction, runtimeConfig)
//      )
//    }
//  }
//
//  override protected def stopExecution() {}
//}

object GenerateLinksTask {
  def empty = new GenerateLinksTask(Traversable.empty, LinkSpecification())
}

/**
 * Created with IntelliJ IDEA.
 * User: stefan
 * Date: 25.09.13
 * Time: 14:54
 * To change this template use File | Settings | File Templates.
 */
object RunSilk extends App {

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

  def executeLinkConfig(sources: Traversable[Source], linkSpec: LinkSpecification)(implicit config: RuntimeConfig) = {
    new GenerateLinksTask(
      sources = sources,
      linkSpec = linkSpec,
      outputs = linkSpec.outputs,
      runtimeConfig = config
    ).apply()
  }

  val sources = List(
    Source("geonames", NewFileDataSource("/home/stefan/Code/diplom-code/ldif-geo/dump/geonames-countries.nt")),
    Source("reegle", NewFileDataSource("/home/stefan/Code/diplom-code/ldif-geo/dump/reegle.nt")))

  case class EvaluationConfig(evalType: EvaluationType, alignmentRef: File, alignmentOut: File,
                              alignmentResults: File, rule: LinkageRule)

  def evaluate(conf: EvaluationConfig) = {

    val linkSpec = LinkSpecification(
      id = "geonamesReegle",
      linkType = "http://www.w3.org/2002/07/owl#sameAs",
      datasets = (Dataset("geonames", "a", SparqlRestriction.fromSparql("a", "?a rdf:type gn:Feature .")),
        Dataset("reegle", "b", SparqlRestriction.fromSparql("b", "?b rdf:type gn:Feature ."))),
      rule = conf.rule,
      outputs = List(new Output("output", new AlignmentApiWriter(conf.alignmentOut))))

    executeLinkConfig(sources, linkSpec)

    Evaluation.eval(conf.evalType, conf.alignmentRef, conf.alignmentOut, conf.alignmentResults)

    val xml = XML.loadFile(conf.alignmentResults)
    val prec = (xml \ "output" \ "precision" text).toDouble
    val recall = (xml \ "output" \ "recall" text).toDouble
    (prec, recall)
  }

  // Input Transformations --------------------

  val optimisticStopWords = List("republic", "of", "bailiwick", "the", "kingdom", "grand duchy", "territory", "and", "principality")

  def transformInputWith(t: Transformer): Input => Input = {
    (in: Input) => TransformInput(transformer = t, inputs = Seq(in))
  }

  val lc = transformInputWith(LowerCaseTransformer())
  val lc2 = transformInputWith(new Transformer {
    def apply(values: Seq[Set[String]]): Set[String] = {
      values.reduce(_ ++ _).map(_.toLowerCase)
    }
  })
  val soundex = transformInputWith(SoundexTransformer())
  val nysiis = transformInputWith(NysiisTransformer())
  val selectShortestValue = transformInputWith(new Transformer {
    def apply(values: Seq[Set[String]]): Set[String] = {
      val sel = values.flatten.toList.sortBy(_.length).headOption.toSet
      //println(f"$values -> $sel")
      sel
    }
  })
  def sw(stopwords: Seq[String]) = transformInputWith(new SimpleTransformer {
    val stopWords = optimisticStopWords
    def evaluate(value: String): String = {
      val str = value.split(" ").filterNot(s => stopWords.contains(s.toLowerCase)).mkString(" ")
      //          if (value.length != str.length) {
      //            println(f"transformed $value to $str")
      //          }
      str
    }
  })


  val inputs = {
    (PathInput(path = Path.parse("?a/gn:name")) :: PathInput(path = Path.parse("?b/gn:name")) :: Nil)
  } map lc2 // map selectShortestValue map sw(optimisticStopWords)

  // Link Rules & Evaluation ----------------------------------

//  val countryCodeRule = LinkageRule(Comparison(
//    id = "codes",
//    required = false,
//    //    weight = 1,
//    //    threshold = 2.0,
//    metric = EqualityMetric(),
//    inputs = (PathInput(path = Path.parse("?a/gn:countryCode")) ::
//      PathInput(path = Path.parse("?b/gn:countryCode")) :: Nil) map lc
//  ))
//
//  val countryCodeEvaluation = EvaluationConfig(SemPRecEvaluation,
//    new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-ref.rdf"),
//    new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-countryCode-equality.rdf"),
//    new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/result-countryCode-semprec.rdf"),
//    countryCodeRule)

  def nameEvaluation(t: Double) = {
    EvaluationConfig(SemPRecEvaluation,
      new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-ref.rdf"),
      new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/align-name-jaroWinkler.rdf"),
      new File("/home/stefan/Code/diplom-code/ldif-geo/geo/alignment/result-name-jaroWinkler-semprec.rdf"),
      LinkageRule(Comparison(
        id = "codes",
        required = false,
        //    weight = 1,
        threshold = t,
        //metric = EqualityMetric(),
        //      metric = JaroWinklerDistance(),
        //      metric = JaroDistanceMetric(),
        //      metric = LevenshteinMetric(),
        //metric = new RelaxedEqualityMetric(),
        //metric = QGramsMetric(q = 3),
        metric = SubStringDistance("5"),
        inputs = inputs
      )))
  }


  // evaluate(countryCodeEvaluation)
  for (i <- 0 to 100) {
    val t = i / 100.0
    val eval = nameEvaluation(t)
    val (prec, recall) = evaluate(eval)
    println(f"${1.0 - t}, $prec, $recall")
  }

}
