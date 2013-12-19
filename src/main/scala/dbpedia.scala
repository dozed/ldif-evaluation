import dispatch._, Defaults._
import org.apache.jena.riot.{RDFLanguages, LangBuilder, Lang}
import org.json4s.JsonAST.{JArray, JValue}
import org.json4s.JsonFormat
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.xml.Elem
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.Exception._

import org.json4s.DefaultReaders._

object DBpedia {

  val keywordUrl = url("http://lookup.dbpedia.org/api/search.asmx/KeywordSearch")
  val sparqlUrl = url("http://dbpedia.org/sparql")

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
      el <- x \ "Description" \ "wikiPageRedirects" \ "@{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
    } yield el.text
    redirects headOption
  }


  def keywordSearch(query: String) = {
    Http(keywordUrl <:< Map("Accept" -> "application/json") <<? Map(
      "QueryClass" -> "",
      "QueryString" -> query) OK as.json4s.Json)
  }

  def sparql(query: String) = {
    val qp = Map("query" -> query, "format" -> "application/ld+json")
    Http(sparqlUrl <<? qp OK as.json4s.Json)
  }

}

case class WikipediaSearchResult(title: String, snippet: String, size: Int)

object AppFormats {
  implicit object WikipediaSearchResultFormat extends JsonFormat[WikipediaSearchResult] {

    def write(obj: WikipediaSearchResult): JValue = ???

    def read(value: JValue): WikipediaSearchResult = {
      val q = for {
        ns <- (value \ "ns").getAs[Int]
        title <- (value \ "title").getAs[String]
        snippet <- (value \ "snippet").getAs[String]
        size <- (value \ "size").getAs[Int]
        timestamp <- (value \ "timestamp").getAs[String]
      } yield WikipediaSearchResult(title, snippet, size)
      q.get
    }

  }
}


object Wikipedia {

  val apiUrl = url("http://en.wikipedia.org/w/api.php")

  import AppFormats._

  def search(query: String) = {
    for {
      json <- Http(apiUrl <<? Map(
        "action" -> "query",
        "list" -> "search",
        "format" -> "json",
        "srsearch" -> query) OK as.json4s.Json)
    } yield json
  }


}

case class SparqlEndpoint(uri: String) {

  val svc = url(uri)

  def stream(query: String, pageSize: Int = 10000, offset: Long = 0, retryCount: Int = 0, retryMax: Int = 10, lang: Lang = Lang.TURTLE): Stream[String] = {
    var q = query
    q += " OFFSET " + offset
    q += " LIMIT " + pageSize
    val qp = Map("query" -> q, "format" -> lang.getContentType.getContentType)

    Thread.sleep(2000)

    def retry(t: Throwable) = {
      println(t.getMessage)
      if (retryCount < retryMax) {
        stream(query, pageSize, offset, retryCount + 1)
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
          lines.toStream #::: stream(query, pageSize, offset + pageSize, 0)
        }
    }
  }

  def query(query: String, lang: Lang = Lang.TURTLE) = {
    val qp = Map("query" -> query, "format" -> lang.getContentType.getContentType)
    Http(svc <<? qp OK as.String)
  }

}

object SparqlImporter extends App {

  val query = """CONSTRUCT {
                |  ?b rdfs:label ?label
                |} WHERE {
                |  ?b dcterms:subject ?x .
                |  { ?b foaf:name ?label . } UNION { ?b rdfs:label ?label . } UNION { ?b dbpprop:name ?label . }
                |  {
                |#  	SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Foods . }
                |#  } UNION {
                |#  	SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Beverages . }
                |#  } UNION {
                |   SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Eukaryotes . }
                |  }
                |}
                | """.stripMargin

  def query2(r: String) = f"""CONSTRUCT {
                            |  <$r> rdfs:label ?label
                            |} WHERE {
                            |  { <$r> foaf:name ?label . } UNION { <$r> rdfs:label ?label . } UNION { <$r> dbpprop:name ?label . }
                            |}
                            |""".stripMargin

  val query3 = """CONSTRUCT {
                 |  ?x rdfs:label ?label ;
                 |    skos:broader ?broader .
                 |} WHERE {
                 |  ?x rdfs:label ?label ;
                 |    skos:broader ?broader .
                 |  {
                 |    SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Foods . }
                 |  } UNION {
                 |    SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Beverages . }
                 |#  } UNION {
                 |#    SELECT DISTINCT(?x) WHERE { ?x skos:broader* category:Organisms . }
                 |  }
                 |}""".stripMargin

  // relevant categories in hierarchy:
  //  category:Foods
  //  category:Beverages -> category:Liquids
  //  category:Food_and_drink
  //  category:Animals
  //  category:Fungi
  //  category:Plants

  val endpoint = SparqlEndpoint("http://dbpedia.org/sparql")
  val jsonld = LangBuilder.create("JSON-LD", "application/ld+json").build()
  RDFLanguages.register(jsonld)

  endpoint.query("""construct {
                  |  ?s ?p dbpedia:Ditalini
                  |} where {
                  |  ?s ?p dbpedia:Ditalini
                  |} LIMIT 10""".stripMargin, lang = jsonld) foreach println

//  val pw = new java.io.PrintWriter("dbpedia-organisms.ttl")
//  endpoint.dump(query) foreach {
//    line =>
//      pw.println(line)
//  }
//  pw.close

}

