import dispatch._, Defaults._
import java.io.File
import org.apache.jena.riot.{RDFLanguages, LangBuilder, Lang}
import org.json4s._
import scala.xml.Elem
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.control.Exception._

import org.json4s.DefaultReaders._

object DBpediaConceptFilter {

  // 800
  // category:1898_ships, category:1985_television_episodes
  val aggregateByYear = """category:\d\d\d\d_.+""".r
  val aggregateByYear2 = """category:.+?_\d\d\d\d""".r

  val relationViaOf = """category:.+?_of_.+""".r

  // _built_in_, _establishments_in_, _established_in_, _disestablished_in_
  val relationViaIn = """category:.+?_in_.+""".r

  // category:1920s_births, category:359_births
  val birthAggregate = """category:.+?_births""".r
  val deathAggregate = """category:.+?_deaths""".r

  // category:2008_albums, category:The_Cure_albums
  val aggregateAlbum = """category:.+?_albums""".r

  def isConcept(c: String): Boolean = !isCategory(c)

  def isCategory(c: String): Boolean = {
    isAggregateByYear(c) || isRelation(c) || isBirthOrDeath(c) || isAlbum(c)
  }

  def isAggregateByYear(c: String): Boolean = {
    (aggregateByYear findFirstIn c) orElse (aggregateByYear2 findFirstIn c) isDefined
  }

  def isRelation(c: String): Boolean = {
    (relationViaOf findFirstIn c) orElse (relationViaIn findFirstIn c) isDefined
  }

  def isBirthOrDeath(c: String): Boolean = {
    (birthAggregate findFirstIn c) orElse (deathAggregate findFirstIn c) isDefined
  }

  def isAlbum(c: String): Boolean = {
    (aggregateAlbum findFirstIn c) isDefined
  }

}

object DBpediaFiles {

  val maxDistance = 77

  val categories = new File("D:/Dokumente/dbpedia2/skos_categories_en.nt")
  val categoryLabels = new File("D:/Dokumente/dbpedia2/category_labels_en.nt")

  val articleLabels = new File("D:/Dokumente/dbpedia2/labels_en.nt")
  val articleCategories = new File("D:/Dokumente/dbpedia2/article_categories_en.nt")

}

object prefixHelper {

  val defaultPrefixes = Map(
    "category" -> "http://dbpedia.org/resource/Category:",
    "dbpedia" -> "http://dbpedia.org/resource/",
    "taaable" -> "http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A",
    "common" -> "http://example.org/common/"
  )

  val taxonomicPredicates = List(
    "http://www.w3.org/2004/02/skos/core#broader",
    "http://purl.org/dc/terms/subject",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf")

  val labelPredicates = List(
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://xmlns.com/foaf/0.1/name",
    "http://dbpedia.org/property/name")

  def shortenUri(fullUri: String): String = {
    defaultPrefixes filter {
      case (_, uriPrefix) => fullUri.startsWith(uriPrefix)
    } headOption match {
      case Some((shortPrefix, uriPrefix)) => shortPrefix + ":" + fullUri.replaceAll(uriPrefix, "")
      case None => fullUri
    }
  }

  def fullUri(shortUri: String): String = {
    defaultPrefixes filter {
      case (shortPrefix, _) => shortUri.startsWith(shortPrefix + ":")
    } headOption match {
      case Some((shortPrefix, uriPrefix)) => uriPrefix + shortUri.replaceAll(shortPrefix + ":", "")
      case None => shortUri
    }
  }

}

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

