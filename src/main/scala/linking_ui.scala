import akka.actor.ActorSystem
import de.fuberlin.wiwiss.silk.config.{LinkSpecification, Dataset}
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity.{Link, Path, SparqlRestriction, Entity}
import de.fuberlin.wiwiss.silk.linkagerule.input.PathInput
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Comparison, DistanceMeasure}
import de.fuberlin.wiwiss.silk.output.{LinkWriter, Output}
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import javax.servlet.ServletContext
import org.apache.jena.riot.{RDFDataMgr, Lang}
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletHolder, DefaultServlet}
import org.eclipse.jetty.webapp.WebAppContext
import org.json4s.DefaultFormats
import org.scalatra.json.JacksonJsonSupport
import org.scalatra.scalate.ScalateSupport
import org.scalatra.{FutureSupport, ScalatraServlet, LifeCycle}
import org.scalatra.servlet.ScalatraListener
import scala.concurrent.ExecutionContext
import SparseDistanceMatrixIO._

object Launcher extends App {

  val server = new Server
  val connector = new SelectChannelConnector
  connector.setPort(8080)
  server.addConnector(connector)

  val context = new WebAppContext
  context.setContextPath("/")

  val resourceBase = "src/main/webapp"
  context.setResourceBase(resourceBase)
  context.setEventListeners(Array(new ScalatraListener))
  context.setInitParameter(ScalatraListener.LifeCycleKey, "Bootstrap")

  val defaultServlet = new DefaultServlet()
  val holder = new ServletHolder(defaultServlet)
  holder.setInitParameter("useFileMappedBuffer", "false")
  context.addServlet(holder, "/")

  server.setHandler(context)

  server.start
  server.join

}

class Bootstrap extends LifeCycle {
  override def init(context: ServletContext): Unit = {
    val res = new MatchingResults
    val system = ActorSystem()
    context.mount(LinkingUI(res, system), "/*")
  }

  override def destroy(context: ServletContext): Unit = {

  }
}

class MatchingResults extends Evaluations {

  val base = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"

  val sources = (Source("taaable", NewFileDataSource(f"file:///$base/taaable-food.rdf", Some(Lang.RDFXML), true)),
    Source("dbpedia", NewFileDataSource(f"file:///$base/dbpedia-foods-combined.ttl", Some(Lang.TURTLE))))

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
    )))

  val entityDescs = linkSpec.entityDescriptions
  val sourceEntities = entities(sources._1, entityDescs._1).toList
  val targetEntities = entities(sources._2, entityDescs._2).toList

  val measures = List(
    "substring" -> SubStringDistance(),
    "qgrams2" -> QGramsMetric(q = 2),
    "jaro" -> JaroDistanceMetric(),
    "jaroWinkler" -> JaroWinklerDistance(),
    "levenshtein" -> LevenshteinMetric(),
    "relaxedEquality" -> new RelaxedEqualityMetric()
  )

  def normalize(s: String): Option[String] = {
    val s1 = s.toLowerCase
    val s2 = s1.replaceAll( """['\(\)\?\.\s_\-]""", "")
    val s3 = s2.trim
    if (!s3.isEmpty) Some(s3) else None
  }

  val distCache = collection.mutable.Map[Int, List[SimEdge]]()

  def distances(i: Int, t: Double = 0.5) = {
    def distance(e1: Entity, e2: Entity, measure: DistanceMeasure) = {
      val v1 = e1.values.flatten flatMap (s => normalize(s))
      val v2 = e2.values.flatten flatMap (s => normalize(s))
      val m = measure(v1, v2)
      if (m < t) {
        Some(m)
      } else {
        None
      }
    }

    if (!distCache.contains(i)) {
      val e1 = sourceEntities(i)

      val d_i = for {
        (e2, j) <- targetEntities.par.zipWithIndex
      } yield {

        val d_ij = for {
          ((label, measure), mi) <- measures.zipWithIndex
          dist <- distance(e1, e2, measure)
        } yield (dist, mi)

        if (d_ij.size > 0) Some(SimEdge(i, j, d_ij)) else None
      }

      val r = d_i.flatten.toList
      println(f"found ${r.size} matches for source entity $i")
      distCache(i) = r
    }

    distCache(i)
  }

  lazy val edges = readMergedEdgeLists(measures map (l => new java.io.File(f"$base/sim-$l.sparse")))

  val (m, n) = (sourceEntities.size, targetEntities.size)

  def isApproximate(t: Double)(e: SimEdge) = e.sim.exists(_._1 <= t)

  def isExact = isApproximate(0.0) _

  val acceptedLinks = collection.mutable.Set[SimEdge]()

  def isAccepted(e: SimEdge) = acceptedLinks.contains(e)

}


case class LinkingUI(res: MatchingResults, system: ActorSystem) extends ScalatraServlet with ScalateSupport with FutureSupport with JacksonJsonSupport {

  override protected val defaultLayoutPath = Some("layout.jade")

  protected implicit def executor: ExecutionContext = system.dispatcher

  implicit val jsonFormats = DefaultFormats

  before() {
    contentType = "text/html"
  }

  get("/") {
    val threshold = params.getAsOrElse[Double]("threshold", 0.0)
    val matches = res.edges.groupBy(_.from)
    val (exact, temp) = matches.partition(_._2.exists(_.sim.exists(_._1 == 0.0)))
    val (approx, nomatches) = temp.partition(_._2.exists(_.sim.exists(_._1 <= threshold)))

    jade("index.jade",
      "measures" -> res.measures,
      "source" -> res.sourceEntities,
      "target" -> res.targetEntities,
      "edges" -> res.edges,
      "exact" -> exact,
      "approx" -> approx,
      "threshold" -> threshold,
      "nomatches" -> nomatches
    )
  }

  get("/match") {
    <ul>{
      for {
        m <- res.acceptedLinks
        s <- res.sourceEntities.lift(m.from)
        t <- res.targetEntities.lift(m.to)
      } yield <li>{f"${s.uri} - ${t.uri}"}</li>
    }</ul>
  }

  delete("/match") {
    res.acceptedLinks.clear
  }

  get("/match/:sourceId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
      threshold <- params.getAs[Double]("threshold").orElse(Some(0.0))
      skipExact <- params.getAs[Boolean]("skipExact").orElse(Some(false))
    } yield {
      val matchesRaw = res.distances(sourceId)
      val accepted = matchesRaw.filter(res.isAccepted)

      val (exact, temp2) = matchesRaw.filter(_.sim.exists(_._1 <= threshold))
        .filter(e => !res.isAccepted(e))
        .sortBy(_.sim.sortBy(_._1).head)
        .partition(res.isExact)
      val (approx, nomatches) = temp2.partition(res.isApproximate(threshold))

      if (skipExact && exact.size > 0) {
        val u = url(f"/match/${sourceId + 1}", Map("threshold" -> threshold, "skipExact" -> skipExact))
        redirect(u)
      }

      jade("match.jade",
        "measures" -> res.measures,
        "sourceEntities" -> res.sourceEntities,
        "targetEntities" -> res.targetEntities,
        "skipExact" -> skipExact,
        "threshold" -> threshold,
        "sourceId" -> sourceId,
        "source" -> source,
        "exact" -> exact,
        "accepted" -> accepted,
        "approx" -> approx,
        "nomatches" -> nomatches,
        "res" -> res)
    }) getOrElse halt(500)
  }

  post("/match/:sourceId/:targetId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      targetId <- params.getAs[Int]("targetId")
      source <- res.sourceEntities.lift(sourceId)
      target <- res.targetEntities.lift(targetId)
      m <- res.distances(sourceId).filter {
        e => e.from == sourceId && e.to == targetId
      } headOption
    } yield {
      println(f"Accepted: $m")
      res.acceptedLinks += m
    }) getOrElse halt(500)
  }

  delete("/match/:sourceId/:targetId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      targetId <- params.getAs[Int]("targetId")
      m <- res.acceptedLinks.filter(e => e.from == sourceId && e.to == targetId) headOption
    } yield {
      println(f"Deleted: $m")
      res.acceptedLinks -= m
    }) getOrElse halt(500)
  }

  get("/dbpedia/:sourceId/usage") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
      limit <- params.getAs[Int]("limit").orElse(Some(5))
    } yield {
      val token = source.uri.replaceAll("http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A", "")
      val q = f"""construct {
                |  dbpedia:$token ?p ?o
                |} where {
                |  dbpedia:$token ?p ?o
                |} LIMIT $limit""".stripMargin
      DBpedia.sparql(q)
    }) getOrElse halt(500)
  }

  get("/dbpedia/:sourceId/reverseUsage") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
      limit <- params.getAs[Int]("limit").orElse(Some(5))
    } yield {
      val token = source.uri.replaceAll("http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A", "")
      val q = f"""construct {
                |  ?s ?p dbpedia:$token
                |} where {
                |  ?s ?p dbpedia:$token
                |} LIMIT $limit""".stripMargin
      DBpedia.sparql(q)
    }) getOrElse halt(500)
  }

  get("/dbpedia/keywordSearch") {
    (for {
      q <- params.get("query")
    } yield DBpedia.keywordSearch(q)) getOrElse halt(500)
  }

  get("/wikipedia/search") {
    (for {
      q <- params.get("query")
    } yield Wikipedia.search(q)) getOrElse halt(500)
  }

  val taaableBase = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"
  val dbpediaBase = "D:/Dokumente/dbpedia2"

  //  val reasoner = ReasonerRegistry.getRDFSReasoner
  //  val model = ModelFactory.createDefaultModel()

  //val taaableGraph = Graph.fromRDFS(RDFDataMgr.loadModel(f"file:///$taaableBase/taaable-food.rdf", Lang.RDFXML))
  // val dbpediaGraph = Graph.fromSKOS(RDFDataMgr.loadModel(f"file:///$dbpediaBase/skos_categories_en.nt", Lang.NTRIPLES))

  val dbpediaGraph = {
    println("loading triples")
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)
    println("converting to graph")
    GraphFactory.fromSKOS(model)
  }

  def n(outer: String) = dbpediaGraph get outer

  get("/dbpedia/path/:from/:to") {
    (for {
      from <- params.get("from")
      to <- params.get("to")
      s <- dbpediaGraph.find(f"http://dbpedia.org/resource/Category:$from")
      t <- dbpediaGraph.find(f"http://dbpedia.org/resource/Category:$to")
      path <- s pathTo t
    } yield {
      path.edges
    }) getOrElse halt(500)
  }

  get("/dbpedia/shortestPath/:from/:to") {
    (for {
      from <- params.get("from")
      to <- params.get("to")
      s <- dbpediaGraph.find(f"http://dbpedia.org/resource/Category:$from")
      t <- dbpediaGraph.find(f"http://dbpedia.org/resource/Category:$to")
      path <- s shortestPathTo t
    } yield {
      path.edges
    }) getOrElse halt(500)
  }

  get("/dbpedia/lcs/:from/:to") {
    (for {
      from <- params.get("from")
      to <- params.get("to")
      lcs <- Alg.leastCommonSubsumer(dbpediaGraph, f"http://dbpedia.org/resource/Category:$from", f"http://dbpedia.org/resource/Category:$to")
    } yield {
      lcs
    }) getOrElse halt(500)
  }

  get("/crunch") {
//    taaableGraph.incomingEdges(Node("http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3AFood")) foreach println
//    dbpediaGraph.incomingEdges(Node("http://dbpedia.org/resource/Category:Food_and_drink")) foreach println





    Alg.test(dbpediaGraph)
    "crunched"
  }

}

