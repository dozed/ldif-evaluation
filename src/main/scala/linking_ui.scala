import akka.actor.ActorSystem
import javax.servlet.ServletContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.akka.AkkaSupport
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

class MatchingResults extends TaaableEvaluation {

  val entityDescs = linkSpec.entityDescriptions
  val sourceEntities = entities(sources._1, entityDescs._1).toList
  val targetEntities = entities(sources._2, entityDescs._2).toList

  val measures = List(
    "substring",
    "qgrams2",
    "jaro",
    "jaroWinkler",
    "levenshtein",
    "relaxedEquality"
  )

  val (m, n) = (sourceEntities.size, targetEntities.size)

  val edges = readMergedEdgeLists(measures map (l => new java.io.File(f"$base/sim-$l.sparse")))

  def isApproximate(t: Double)(e: Edge) = e.sim.exists(_._1 <= t)

  def isExact = isApproximate(0.0) _

  val acceptedLinks = collection.mutable.Set[Edge]()

  def isAccepted(e: Edge) = acceptedLinks.contains(e)

}


case class LinkingUI(res: MatchingResults, system: ActorSystem) extends ScalatraServlet with ScalateSupport with FutureSupport {

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
    <ul>
      {for (m <- res.acceptedLinks) yield <li>
      {f"${m.from} - ${m.to}"}
    </li>}
    </ul>
  }

  get("/match/:sourceId") {
    (for {
      sourceId <- params.getAs[Int]("sourceId")
      source <- res.sourceEntities.lift(sourceId)
      threshold <- params.getAs[Double]("threshold").orElse(Some(0.0))
      skipExact <- params.getAs[Boolean]("skipExact").orElse(Some(false))
    } yield {
      val matches = res.edges.filter(_.from == sourceId)
        .filter(_.sim.exists(_._1 <= threshold))
        .sortBy(_.sim.sortBy(_._1).head)
      val (exact, temp) = matches.partition(res.isExact)
      val (accepted, temp2) = temp.partition(res.isAccepted)
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
      m <- res.edges.filter {
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

