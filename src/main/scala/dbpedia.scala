import dispatch._, Defaults._
import scala.xml.Elem
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.Exception._

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


case class SparqlEndpoint(uri: String) {

  val svc = url(uri)

  def dump(query: String, pageSize: Int = 5000, offset: Long = 0, retryCount: Int = 0, retryMax: Int = 10): Stream[String] = {
    var q = query
    q += " OFFSET " + offset
    q += " LIMIT " + pageSize
    val qp = Map("query" -> q, "format" -> "text/turtle")

    def retry(t: Throwable) = {
      if (retryCount < retryMax) {
        dump(query, pageSize, offset, retryCount + 1)
      } else {
        throw t
      }
    }

    val f = Http(svc <<? qp OK as.String) map (_.split("\n").toList) either

    allCatch either {
      Await.result(f, 2 minutes)
    } match {
      case Left(t) => retry(t)
      case Right(Left(t)) => retry(t)
      case Right(Right(lines)) =>
        if (lines.size == 0 || (lines.size == 1 && lines(0).startsWith("# Empty"))) {
          Stream.empty
        } else {
          lines.toStream #::: dump(query, pageSize, offset + pageSize, 0)
        }
    }
  }

}

object SparqlImporter extends App {

  val query = """CONSTRUCT {
                |  ?b rdfs:label ?label
                |} WHERE {
                |  ?b dcterms:subject ?x .
                |  { ?b foaf:name ?label . } UNION { ?b rdfs:label ?label . } UNION { ?b dbpprop:name ?label . }
                |  {
                |    SELECT ?x WHERE { ?x skos:broader* category:Foods . }
                |  } UNION {
                |    SELECT ?x WHERE { ?x skos:broader* category:Beverages . }
                |  }
                |}
                |""".stripMargin

  def query2(r: String) = f"""CONSTRUCT {
                            |  <$r> rdfs:label ?label
                            |} WHERE {
                            |  { <$r> foaf:name ?label . } UNION { <$r> rdfs:label ?label . } UNION { <$r> dbpprop:name ?label . }
                            |}
                            |""".stripMargin

  val endpoint = SparqlEndpoint("http://dbpedia.org/sparql")

  val pw = new java.io.PrintWriter("foods-labels.ttl")

  endpoint.dump(query) foreach { line =>
    println(line)
    pw.println(line)
    Thread.sleep(2000)
  }
  pw.close

}