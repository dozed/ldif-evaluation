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
import org.apache.jena.riot.Lang
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
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
    Source("dbpedia", NewFileDataSource(f"file:///$base/dbpedia-foods-labels.ttl", Some(Lang.TURTLE))))

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

  val distCache = collection.mutable.Map[Int, List[Edge]]()

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

        if (d_ij.size > 0) Some(Edge(i, j, d_ij)) else None
      }

      val r = d_i.flatten.toList
      println(f"found ${r.size} matches for source entity $i")
      distCache(i) = r
    }

    distCache(i)
  }

  lazy val edges = readMergedEdgeLists(measures map (l => new java.io.File(f"$base/sim-$l.sparse")))

  val (m, n) = (sourceEntities.size, targetEntities.size)

  def isApproximate(t: Double)(e: Edge) = e.sim.exists(_._1 <= t)

  def isExact = isApproximate(0.0) _

  val acceptedLinks = collection.mutable.Set[Edge]()

  def isAccepted(e: Edge) = acceptedLinks.contains(e)

}


case class LinkingUI(res: MatchingResults, system: ActorSystem) extends ScalatraServlet with ScalateSupport with FutureSupport {

  override protected val defaultLayoutPath = Some("layout.jade")

  protected implicit def executor: ExecutionContext = system.dispatcher

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
        t <- res.sourceEntities.lift(m.to)
      } yield <li>{f"${s.uri} - ${s.uri}"}</li>
    }</ul>
  }

  get("/match/:sourceId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
      threshold <- params.getAs[Double]("threshold").orElse(Some(0.0))
      skipExact <- params.getAs[Boolean]("skipExact").orElse(Some(false))
    } yield {
      val matches = res.distances(sourceId)
        .filter(_.sim.exists(_._1 <= threshold))
        .sortBy(_.sim.sortBy(_._1).head)
      val (accepted, temp) = matches.partition(res.isAccepted)
      val (exact, temp2) = temp.partition(res.isExact)
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

  get("/dbpedia/redirect/:sourceId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
    } yield {
      val token = source.uri.replaceAll("http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A", "")
      for {
        redirectUrl <- DBpedia.redirect(token)
      } yield redirectUrl
    }) getOrElse halt(500)
  }

}

