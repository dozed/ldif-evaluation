import com.hp.hpl.jena.rdf.model.{Resource, Model}
import java.io.{BufferedInputStream, FileInputStream, PrintWriter}
import org.apache.any23.io.nquads.NQuadsParser
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

import org.openrdf.model.Statement
import org.openrdf.rio.RDFHandler

import scalax.collection.Graph
import scalax.collection.GraphTraversal
import scalax.collection.GraphTraversal.VisitorReturn
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.edge.WDiEdge
import scalax.collection.edge.Implicits._

object PrefixHelper {

  val defaultPrefixes = Map(
    "category" -> "http://dbpedia.org/resource/Category:",
    "dbpedia" -> "http://dbpedia.org/resource/",
    "taaable" -> "http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A"
  )

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

object GraphFactory {

  import PrefixHelper._

  def from(model: Model): Graph[String, WDiEdge] = {
    val graph = scalax.collection.mutable.Graph[String, WDiEdge]()
    val predicates = List(
      model.getProperty("http://www.w3.org/2004/02/skos/core#broader"),
      model.getProperty("http://purl.org/dc/terms/subject"),
      model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf"))

    predicates foreach {
      p =>
        for (st <- model.listStatements(null, p, null)) {
          val e1 = shortenUri(st.getSubject.asResource.getURI)
          val e2 = shortenUri(st.getObject.asResource.getURI)
          graph += e1 ~> e2 % 1
        }
    }

    graph
  }

  def fromWikiTaxonomy: Graph[String, DiEdge] = {
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/wikipediaOntology.ttl", Lang.TURTLE)

    val graph = scalax.collection.mutable.Graph[String, DiEdge]()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    val rdfsLabel = model.getProperty("http://www.w3.org/2000/01/rdf-schema#label")

    def label(x: Resource) = {
      model.listStatements(x, rdfsLabel, null).next().getObject.asLiteral().getString
    }

    for (x <- model.listSubjects) {
      graph += label(x)

      for (st <- model.listStatements(x, subClassOf, null)) {
        graph += label(x) ~> label(st.getObject.asResource)
      }
    }

    graph

    //    import scalax.collection.io.dot._

    //    println("exporting as dot")
    //    val root = DotRootGraph(directed = true,
    //      id = None,
    //      kvList = Seq(DotAttr("attr_1", """"one""""),
    //        DotAttr("attr_2", "<two>")))
    //
    //    def edgeTransformer(innerEdge: Graph[String, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
    //      val edge = innerEdge.edge
    //      Some(root, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    //    }
    //
    //    val str = new Export(graph).toDot(root, edgeTransformer)
    //    val pw = new PrintWriter("wikiTaxonomy.dot")
    //    pw.println(str)
    //    pw.close
  }

}

object Alg {

  val maxDistance = 77

  // type DiHyperEdgeLikeIn[N] = DiHyperEdgeLike[N] with EdgeCopy[DiHyperEdgeLike] with EdgeIn[N,DiHyperEdgeLike]

  def merge[N](s: N, t: N) = {
    List((s ~> t % 0), (t ~> s % 0))
  }

  def weight[N](p: List[(N, Long)]): Long = p.foldLeft[Long](0)(_ + _._2)

  def shortestPath[N, E[N] <: DiHyperEdgeLikeIn[N]](g: Graph[N, E], s: N, t: N): Option[List[(N, Long)]] = {
    val (_, backlinks) = shortestPaths(g, s)
    if (backlinks.contains(t)) Some(backtrackPath0(s, t, backlinks))
    else None
  }

  def shortestPaths[N, E[N] <: DiHyperEdgeLikeIn[N]](g: Graph[N, E], s: N): (Map[N, Long], Map[N, (N, Long)]) = {
    val d = collection.mutable.Map[N, Long]()
    val Q = collection.mutable.PriorityQueue[N]()(Ordering.by(d.apply))
    val backlinks = collection.mutable.Map[N, (N, Long)]()

    Q += s
    d(s) = 0

    while (!Q.isEmpty) {
      val u = Q.dequeue

      for (e <- g.get(u).outgoing) {
        val v = e.toEdgeIn._2
        val alt = d(u) + e.weight

        if (!d.contains(v) || alt < d(v)) {
          backlinks(v) = (u, e.weight)
          d(v) = alt
          Q.remove(v)
          Q.enqueue(v)
        }
      }
    }

    (d.toMap, backlinks.toMap)
  }

  private def backtrackPath0[N](from: N, to: N, backlinks: Map[N, (N, Long)]): List[(N, Long)] = {
    def path0(v: N, path: List[(N, Long)]): List[(N, Long)] = {
      if (v == from) path
      else {
        val (u, weight) = backlinks(v)
        path0(u, (u, weight) :: path)
      }
    }
    path0(to, List.empty)
  }

  def lcsCandidates[N, E[N] <: WDiEdge[N]](g: Graph[N, E], s: N, t: N): List[(N, List[(N, Long)], List[(N, Long)])] = {
    val (d_s, backlinks_s) = shortestPaths(g, s)

    val d_t = collection.mutable.Map[N, Long](t -> 0)
    val backlinks_t = collection.mutable.Map[N, (N, Long)]()
    val candidates = collection.mutable.HashSet[N]()
    val Q = collection.mutable.PriorityQueue[N]()(Ordering.by(d_t.apply))

    Q += t

    while (!Q.isEmpty) {
      val u = Q.dequeue()

      for (e <- g.get(u).outgoing) {
        val v = e._2
        val alt = d_t(u) + e.weight

        if (!d_t.contains(v) || alt < d_t(v)) {
          d_t(v) = alt
          backlinks_t(v) = (u, e.weight)

          // dont expand if it is reachable from s => a lcs candidate
          if (d_s.contains(v)) {
            candidates += v
          } else {
            Q.remove(v)
            Q.enqueue(v)
          }
        }

      }
    }

    candidates.map {
      v =>
        (v, backtrackPath0(s, v, backlinks_s.toMap), backtrackPath0(t, v, backlinks_t.toMap))
    }.toList sortBy (l => weight(l._2) + weight(l._3))
  }

  def lcs[N](g: Graph[N, WDiEdge], s: N, t: N): Option[(N, List[(N, Long)], List[(N, Long)])] = {
    lcsCandidates(g, s, t).headOption
  }

  def structuralCotopic[N](g: Graph[N, WDiEdge], s: N, t: N): Double = {
    lcs(g, s, t) match {
      case Some((_, p1, p2)) => p1.foldLeft[Long](0)(_ + _._2) + p2.foldLeft[Long](0)(_ + _._2)
      case None => 100000
    }
  }

  def structuralCotopicNormalized[N](g: Graph[N, WDiEdge], s: N, t: N): Double = {
    structuralCotopic(g, s, t) / maxDistance
  }

  def wuPalmer[N](g: Graph[N, WDiEdge], root: N, s: N, t: N): Double = {
    val (l, p1, p2) = lcs(g, s, t).get

    shortestPath(g, l, root) match {
      case Some(p) =>
        val w1 = weight(p1)
        val w2 = weight(p2)
        val wl = weight(p)
        2.0 * wl / (w1 + w2 + 2 * wl)
      case None => 100000
    }
  }

  def subsumers[N](g: Graph[N, WDiEdge], x: N): Set[N] = {
    val subs = new collection.mutable.HashSet[N]()
    g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = {
      n =>
        subs += n
        VisitorReturn.Continue
    })
    subs.toSet
  }

  def subsumerGraph[N](g: Graph[N, WDiEdge], x: N)(implicit m: Manifest[N]): Graph[N, WDiEdge] = {
    val g2 = scalax.collection.mutable.Graph[N, WDiEdge]()

    g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e => g2 += e
    })

    g2
  }

  def pathsTo[N](g: Graph[N, WDiEdge], s: N, t: N): List[List[(N, N)]] = {
    val backlinks = collection.mutable.Map[N, List[N]]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e =>
        val from: N = e.from
        val to: N = e.to

        backlinks(to) = from :: backlinks.getOrElseUpdate(to, List.empty)
    })

    backlinks(s) = List.empty

    val buf = collection.mutable.ArrayBuffer[List[(N, N)]]()

    def backtrackPaths(v: N, current: List[(N, N)] = List.empty) {
      if (v == s) {
        buf += current
      } else {
        for {
          u <- backlinks(v).par
          e = (u, v)
          if (!current.contains(e))
        } backtrackPaths(u, e :: current)
      }
    }

    backtrackPaths(t)

    buf.toList
  }

  def exportAsDot[N](g: Graph[N, WDiEdge]): String = {
    import scalax.collection.io.dot._

    val root = DotRootGraph(directed = true, id = None)

    def edgeTransformer(innerEdge: Graph[N, WDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString.replace("http://dbpedia.org/resource/Category:", ""), edge.to.toString.replace("http://dbpedia.org/resource/Category:", ""), Nil))
    }

    new Export(g).toDot(root, edgeTransformer)
  }

  def extractEnvironment[N](g: Graph[N, WDiEdge], s: N, maxDepth: Int)(implicit m: Manifest[N]) = {
    val g2 = scalax.collection.mutable.Graph[N, WDiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true, maxDepth = maxDepth)(edgeVisitor = {
      e =>
        g2 += e
    })

    g2
  }

  def test(g: Graph[String, WDiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
    //    val e1 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Blue_cheeses", 5)
    //    val e2 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Potatoes", 5)
    //    exportAsDot(e1 ++ e2)
  }

}

object TestDataSet {

  import PrefixHelper._

  def extractTypes(subjects: List[String]): Map[String, Set[String]] = {

    val typeMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val s = p1.getSubject.stringValue
        if (subjects.contains(s)) {
          val p = p1.getPredicate.stringValue
          val o = p1.getObject.stringValue
          typeMap(s) = typeMap.getOrElseUpdate(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    val in = new BufferedInputStream(new FileInputStream("D:/Dokumente/dbpedia2/article_categories_en.nt"))
    parser.parse(in, "http://dbpedia.org/resource/")

    typeMap.toMap
  }

  def generate = {

    val instances = List(
      "http://dbpedia.org/resource/Celery",
      "http://dbpedia.org/resource/Cel-Ray",
      "http://dbpedia.org/resource/Celery_salt",
      "http://dbpedia.org/resource/Celery_Victor",
      "http://dbpedia.org/resource/Celery_cabbage",
      "http://dbpedia.org/resource/Celeriac",
      "http://dbpedia.org/resource/Celebrity_(tomato)"
    )

    println("extracting instance types")
    val typeMap = TestDataSet.extractTypes(instances).map {
      case (k, v) =>
        (k.replaceAll("http://dbpedia.org/resource/", "dbpedia:"), v.map(_.replaceAll("http://dbpedia.org/resource/Category:", "category:")))
    }

    println("loading hierarchy triples")
    val model = RDFDataMgr.loadModel(f"file:///D:/Dokumente/dbpedia2/skos_categories_en.nt", Lang.NTRIPLES)

    println("converting hierarchy to graph")
    val g = GraphFactory.from(model)

    println("extracting transitive types")
    val transitiveTypes = collection.mutable.HashSet[(String, String)]()
    typeMap.map(_._2).flatten foreach {
      x =>
        g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
          e =>
            val u: String = e._1
            val v: String = e._2
            if (!transitiveTypes.contains((u, v))) {
              transitiveTypes += ((u, v))
            }
        })
    }

    val pw = new PrintWriter("test-cellery.nt")

    def write(s: String, p: String, o: String) {
      val ls = f"<${fullUri(s)}> <$p> <${fullUri(o)}> ."
      println(ls)
      pw.println(ls)
    }

    typeMap foreach {
      case (x, ys) =>
        ys foreach (y => write(x, "http://purl.org/dc/terms/subject", y))
    }

    transitiveTypes foreach {
      case (x, y) => write(x, "http://www.w3.org/2004/02/skos/core#broader", y)
    }

    pw.close

  }

}

object GraphTest extends App {

  import PrefixHelper._
  import Alg._

  def wikiTaxonomyToDot {
    println("loading triples")
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/wikipediaOntology.ttl", Lang.TURTLE)

    val graph = scalax.collection.mutable.Graph[String, WDiEdge]()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    val rdfsLabel = model.getProperty("http://www.w3.org/2000/01/rdf-schema#label")

    def label(x: Resource) = {
      model.listStatements(x, rdfsLabel, null).next().getObject.asLiteral().getString
    }

    println("building graph")
    for (x <- model.listSubjects) {
      graph += label(x)

      for (st <- model.listStatements(x, subClassOf, null)) {
        graph += label(x) ~> label(st.getObject.asResource) % 1
      }
    }

    import scalax.collection.io.dot._

    println("exporting as dot")
    val root = DotRootGraph(directed = true,
      id = None,
      kvList = Seq(DotAttr("attr_1", """"one""""),
        DotAttr("attr_2", "<two>")))

    def edgeTransformer(innerEdge: Graph[String, WDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    }

    val str = new Export(graph).toDot(root, edgeTransformer)
    val pw = new PrintWriter("wikiTaxonomy.dot")
    pw.println(str)
    pw.close
  }

  //  wikiTaxonomyToDot
  //  val g = Graph(1 ~> 2, 1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 7, 7 ~> 8, 5 ~> 6, 6 ~> 4, 6 ~> 1, 6 ~> 8, 2 ~> 8)
  //  println(Alg.lcsCandidates(g, 1, 5))

  //  val g = Graph(1 ~> 3, 1 ~> 4, 2 ~> 5, 2 ~> 12, 3 ~> 6, 4 ~> 7, 5 ~> 8, 6 ~> 9, 7 ~> 11, 8 ~> 11, 9 ~> 12, 10 ~> 12, 11 ~> 10)
  //  println(Alg.lcsCandidates(g, 1, 2))
  //  println(Alg.pathsTo(g, 1, 12))
  //  println(Alg.pathsTo(g, 2, 12))

  // val g = Graph(1 ~> 3, 1 ~> 4, 2 ~> 4, 3 ~> 5, 4 ~> 6, 5 ~> 7, 6 ~> 8, 7 ~> 8)
  //  val g = Graph(1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 5, 4 ~> 6, 6 ~> 7, 6 ~> 8, 8 ~> 9)
  //    val g = Graph(1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 5, 4 ~> 6, 6 ~> 7, 6 ~> 8, 8 ~> 9,
  //      1 ~> 20, 20 ~> 21, 21 ~> 22, 22 ~> 23, 23 ~> 24, 24 ~> 25, 25 ~> 26, 26 ~> 27, 27 ~> 28, 28 ~> 29, 29 ~> 30, 30 ~> 42,
  //      42 ~> 7, 42 ~> 8, 42 ~> 9)
  //    Alg.lcsCandidates(g, 1, 2) map { case (v, p1, p2) =>
  //      val q1 = g.get(1) shortestPathTo g.get(v)
  //      val q2 = g.get(2) shortestPathTo g.get(v)
  //      println(f"$v - $q1 - $q2 - $p1 - $p2")
  //    }

  val instances = List(
    "http://dbpedia.org/resource/Celery",
    "http://dbpedia.org/resource/Cel-Ray",
    "http://dbpedia.org/resource/Celery_salt",
    "http://dbpedia.org/resource/Celery_Victor",
    "http://dbpedia.org/resource/Celery_cabbage",
    "http://dbpedia.org/resource/Celeriac",
    "http://dbpedia.org/resource/Celebrity_(tomato)"
  )

  val taaable = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.ttl", Lang.TURTLE))
  val dbpedia = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/celery/test-celery.nt", Lang.NTRIPLES))

  val g = dbpedia ++ taaable ++
    merge("taaable:Food", "category:Food_and_drink") ++
    merge("taaable:Vegetable", "category:Vegetables") ++
    merge("taaable:Stalk_vegetable", "category:Stem_vegetables") ++
    merge("taaable:Leaf_vegetable", "category:Leaf_vegetables") +
    ("category:Food_and_drink" ~> "common:Root" % 1) + ("taaable:Food" ~> "common:Root" % 1)

  for {
    x <- instances.par
  } {
    val d1 = structuralCotopic(g, "taaable:Celery", shortenUri(x))
    val d2 = 1.0 - wuPalmer(g, "common:Root", "taaable:Celery", shortenUri(x))
    val (l, p1, p2) = lcs(g, "taaable:Celery", shortenUri(x)).get
    println(f"$x $d1 $d2 (via $l - $d1 - $d2)")
  }


  // TestDataSet.generate
  //  val model = RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/test-cellery.nt", Lang.NTRIPLES)
  //  val g = from(model)
  //
  //  val g2 = scalax.collection.mutable.Graph[String, WDiEdge]()
  //
  //  g.get("category:Foods").traverse(direction = GraphTraversal.Predecessors, breadthFirst = true)(edgeVisitor = {
  //    e => g2 += e
  //  })
  //
  //  val s = exportAsDot(g2)
  //  val pw = new PrintWriter("test-cellery-2.dot")
  //  pw.println(s)
  //  pw.close


  //  var min = 100000
  //  var max = 0
  //
  //  val dist = collection.mutable.Map[Int, Int]()
  //
  //  for {
  //    s <- g.nodes.par
  //    t <- g.nodes
  //    (v, p1, p2) <- lcsCandidates(g, s.toString, t.toString)
  //  } yield {
  //    val len = p1.size + p2.size
  //    dist(len) = dist.getOrElseUpdate(len, 0) + 1
  //    if (len < min) {
  //      println(f"new minimum: $len - $v - $p1 - $p2")
  //      min = len
  //      println(dist)
  //    }
  //    if (len > max) {
  //      println(f"new maximum: $len - $v - $p1 - $p2")
  //      max = len
  //      println(dist)
  //    }
  //  }
  //
  //  println(f"min: $min max: $max")
  //  println(dist)

  //  val s = "category:Blue_cheeses"
  //  val t = "category:Milk"
  //
  //  lcsCandidates(g, s, t) map {
  //    case (v, p1, p2) =>
  //      val q1 = g.get(s) shortestPathTo g.get(v)
  //      val q2 = g.get(t) shortestPathTo g.get(v)
  //      println(f"$v - $q1 - $q2 - $p1 - $p2")
  //  }

}