import dispatch._, Defaults._
import scala.xml.Elem

object DBpedia {

  def resource(token: String) = {
    val svc = url(f"http://dbpedia.org/data/$token.xml") <:< Map("Accept" -> "application/rdf+xml")
    Http(svc OK as.xml.Elem)
  }

  def redirect(token: String) = {
    for {
      xml <- resource(token)
    } yield extractRedirect(xml)
  }

  def extractRedirect(x: Elem) = {
    val redirects = for {
      el <- x  \ "Description" \ "wikiPageRedirects" \ "@{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
    } yield el.text
    redirects headOption
  }

}

