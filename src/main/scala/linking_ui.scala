import javax.servlet.ServletContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.scalate.ScalateSupport
import org.scalatra.{ScalatraServlet, LifeCycle}
import org.scalatra.servlet.ScalatraListener
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
    context.mount(LinkingUI(res), "/*")
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

  val edges = readMergedEdgeLists(measures map (l =>new java.io.File(f"$base/sim-$l.sparse")))

}

case class LinkingUI(var res: MatchingResults) extends ScalatraServlet with ScalateSupport {

  before() {
    contentType = "text/html"
  }

  get("/") {
    val threshold = params.getAsOrElse[Double]("threshold", 0.0)
    val matches = res.edges.filter(_.sim.exists(_._1 <= threshold))
      .sortBy(_.sim.sortBy(_._1).head)
    val (exact, approx) = matches.partition(_.sim.exists(_._1 == 0.0))

    jade("index.jade",
      "measures" -> res.measures,
      "source" -> res.sourceEntities,
      "target" -> res.targetEntities,
      "edges" -> res.edges,
      "exact" -> exact,
      "approx"-> approx,
      "threshold" -> threshold
    )
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
      val (exact, approx) = matches.partition(_.sim.exists(_._1 == 0.0))

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
        "approx" -> approx)
    }) getOrElse halt(500)
  }

}

