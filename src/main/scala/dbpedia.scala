import scala.concurrent.Future
import scala.xml.Elem

import dispatch._, Defaults._

object DBpedia {

  def resource(token: String): Future[Elem] = {
    val svc = url(f"http://dbpedia.org/resource/$token.xml") <:< Map("Accept" -> "application/rdf+xml")
    Http(svc OK as.xml.Elem)
  }

}

