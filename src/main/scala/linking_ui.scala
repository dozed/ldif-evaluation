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
    context.mount(new LinkingUI(), "/*")
  }

  override def destroy(context: ServletContext): Unit = {

  }
}

object SimilarityStorage extends TaaableEvaluation {

  val entityDescs = linkSpec.entityDescriptions
  val sourceEntities = entities(sources._1, entityDescs._1).toList
  val targetEntities = entities(sources._2, entityDescs._2).toList

  val measures = List("substring", "levenshtein", "relaxedEquality")

  val (m, n) = (2165, 29212)
  val mats = measures map (l => readSparseDistanceMatrix(new java.io.File(f"sim-$l.sparse"), m, n))

}

class LinkingUI extends ScalatraServlet with ScalateSupport {

  before() {
    contentType = "text/html"
  }

  get("/") {
    jade("index.jade",
      "measures" -> SimilarityStorage.measures,
      "mats" -> SimilarityStorage.mats,
      "sourceEntities" -> SimilarityStorage.sourceEntities,
      "targetEntities" -> SimilarityStorage.targetEntities)
  }

}

