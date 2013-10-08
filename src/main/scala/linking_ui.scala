import breeze.linalg.DenseVector
import javax.servlet.ServletContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.scalate.ScalateSupport
import org.scalatra.{ScalatraServlet, LifeCycle}
import org.scalatra.servlet.ScalatraListener
import scala.collection.mutable.ArrayBuffer
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

  val measures = List("substring", "levenshtein", "relaxedEquality")

  val (m, n) = (sourceEntities.size, targetEntities.size)
  val mats = measures map (l => readSparseDistanceMatrix(new java.io.File(f"sim-$l.sparse"), m, n))

  val t = 0.1
  val perfect = ArrayBuffer[(Int, List[(String, Int, Double)])]()
  val approx = ArrayBuffer[(Int, List[(String, Int, Double)])]()
  val nomatch = ArrayBuffer[(Int, List[(String, Int, Double)])]()

  def extract(rows: List[(String, List[(Int, Double)])], t: Double) = for {
    (measureLabel, row) <- rows
    (j, dist) <- row
    if (dist <= t)
  } yield (measureLabel, j, dist)

  for (row <- (0 to m - 1)) {

    // extract for a specific source entity a map of results for each distance measure
    //
    val rows: List[(String, List[(Int, Double)])] = (for {
      (measure, mat) <- (measures zip mats)
    } yield (measure -> mat(row, ::).toDenseVector.iterator.toList))
    println(row)

    val p = extract(rows, 0.0)

    if (p.size > 0) {
      perfect += ((row, extract(rows, 0.3)))
    } else {
      val a = extract(rows, 0.1)
      if (a.size > 0) {
        approx += ((row, extract(rows, 0.3)))
      } else {
        nomatch += ((row, extract(rows, 0.3)))
      }
    }
  }

}

case class LinkingUI(var res: MatchingResults) extends ScalatraServlet with ScalateSupport {

  before() {
    contentType = "text/html"
  }

  get("/") {
    jade("index.jade",
      "measures" -> res.measures,
      "mats" -> res.mats,
      "source" -> res.sourceEntities,
      "target" -> res.targetEntities,
      "perf" -> res.perfect,
      "approx" -> res.approx,
      "nomatch" -> res.nomatch
    )
  }

}

