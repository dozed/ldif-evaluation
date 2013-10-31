import com.hp.hpl.jena.rdf.model.{Resource, Model}
import java.io.PrintWriter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

import scalax.collection.GraphTraversal.VisitorReturn
import scalax.collection.{GraphEdge, GraphTraversal, Graph}
import scalax.collection.GraphPredef._, scalax.collection.GraphEdge._

object GraphFactory {

  //  def fromRDFS(model: Model) = {
  //    val graph = new Graph()
  //    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
  //
  //    for (x <- model.listSubjects) {
  //      val s = Node(x.getURI)
  //      graph.addNode(s)
  //
  //      for (st <- model.listStatements(x, subClassOf, null)) {
  //        val e = Edge("is-a", s.id, st.getObject.asResource.getURI)
  //        graph.addEdge(e)
  //      }
  //    }
  //
  //    graph
  //  }

  def fromSKOS(model: Model) = {
    val graph = scalax.collection.mutable.Graph[String, DiEdge]()
    val broader = model.getProperty("http://www.w3.org/2004/02/skos/core#broader")

    for (x <- model.listSubjects) {
      graph += x.getURI

      for (st <- model.listStatements(x, broader, null)) {
        graph += x.getURI ~> st.getObject.asResource.getURI
      }
    }

    graph
  }

}

object Alg {

  def lcsCandidates[N](g: Graph[N, DiEdge], s: N, t: N): List[(N, List[N], List[N])] = {
    val Q = collection.mutable.Queue[N](s, t)
    val d = collection.mutable.Map[N, Int](s -> 0, t -> 0)
    val fromS = collection.mutable.HashSet(s)
    val fromT = collection.mutable.HashSet(t)

    val candidates = collection.mutable.ArrayBuffer[N]()

    val linksS = collection.mutable.Map[N, N]()
    val linksT = collection.mutable.Map[N, N]()

    while (!Q.isEmpty) {

      val u = Q.dequeue()

      for (v <- g.get(u).diSuccessors) {
        if (!d.contains(v)) {
          // expand if required
          d(v) = d(u) + 1
          Q += v
        }

        // reachable from s and t => is a candidate for lcs
        // ignore transitive visits
        val stReachable1 = fromT.contains(v) && (fromS.contains(u) && !fromT.contains(u))
        val stReachable2 = fromS.contains(v) && (!fromS.contains(u) && fromT.contains(u))

        if (stReachable1 || stReachable2) {
          candidates += v
        }

        // update backlinks if required
        if (fromS(u) && !linksS.contains(v)) linksS(v) = u
        if (fromT(u) && !linksT.contains(v)) linksT(v) = u

        // propagate reachability
        if (!fromS.contains(v)) fromS(v) = fromS(u)
        if (!fromT.contains(v)) fromT(v) = fromT(u)
      }

    }

    def backtrackPath(from: N, to: N, backlinks: Map[N, N]) = {
      def path0(v: N, path: List[N]): List[N] = {
        if (v == from) path
        else path0(backlinks(v), backlinks(v) :: path)
      }
      path0(to, List.empty)
    }

    // candidates.toSet
    candidates.toSet[N].toList map {
      v =>
        (v, backtrackPath(s, v, linksS.toMap), backtrackPath(t, v, linksT.toMap))
    } sortBy (l => l._2.size + l._3.size)
  }

  def lcs[N](g: Graph[N, DiEdge], s: N, t: N): Option[(N, List[N], List[N])] = {
    lcsCandidates(g, s, t).headOption
  }

  def pathsTo[N](g: Graph[N, DiEdge], s: N, t: N): List[List[(N, N)]] = {
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

  def exportSubsumers[N](g: Graph[N, DiEdge], s: N)(implicit m: Manifest[N]) {
    val g2 = scalax.collection.mutable.Graph[N, DiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e =>
        g2 += e
    })

    exportAsDot(g)
  }

  def exportAsDot[N](g: Graph[N, DiEdge]) {
    import scalax.collection.io.dot._

    val root = DotRootGraph(directed = true,
      id = None,
      kvList = Seq(DotAttr("attr_1", """"one""""),
        DotAttr("attr_2", "<two>")))

    def edgeTransformer(innerEdge: Graph[N, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString.replace("http://dbpedia.org/resource/Category:", ""), edge.to.toString.replace("http://dbpedia.org/resource/Category:", ""), Nil))
    }

    val str = new Export(g).toDot(root, edgeTransformer)
    val pw = new PrintWriter("categories.dot")
    pw.println(str)
    pw.close
  }

  def extractEnvironment[N](g: Graph[N, DiEdge], s: N, maxDepth: Int)(implicit m: Manifest[N]) = {
    val g2 = scalax.collection.mutable.Graph[N, DiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true, maxDepth = maxDepth)(edgeVisitor = {
      e =>
        g2 += e
    })

    g2
  }

  def extractHierarchy {
    val model = RDFDataMgr.loadModel(f"file:///D:/Dokumente/dbpedia2/skos_categories_en.nt", Lang.NTRIPLES)

    val broader = model.getProperty("http://www.w3.org/2004/02/skos/core#broader")
    val visited = collection.mutable.Set[Resource]()
    val pw = new PrintWriter("dbpedia-foods-categories-3.nt")

    def followBroaderInverse(x: Resource) {
      for (st <- model.listStatements(null, broader, x)) {
        val s = st.getSubject.asResource
        val o = st.getObject.asResource

        val str = f"<${s.getURI}> <http://www.w3.org/2004/02/skos/core#broader> <${o.getURI}> ."
        println(str)
        pw.println(str)

        if (!visited.contains(s)) {
          visited += s
          followBroaderInverse(s)
        }
      }
    }

    val x = model.getResource("http://dbpedia.org/resource/Category:Food_and_drink")
    followBroaderInverse(x)

    pw.close
  }

  def test(g: Graph[String, DiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
    //    val e1 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Blue_cheeses", 5)
    //    val e2 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Potatoes", 5)
    //    exportAsDot(e1 ++ e2)
  }

}


object GraphTest extends App {

  def wikiTaxonomyToDot {
    println("loading triples")
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/wikipediaOntology.ttl", Lang.TURTLE)

    val graph = scalax.collection.mutable.Graph[String, DiEdge]()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    val rdfsLabel = model.getProperty("http://www.w3.org/2000/01/rdf-schema#label")

    def label(x: Resource) = {
      model.listStatements(x, rdfsLabel, null).next().getObject.asLiteral().getString
    }

    println("building graph")
    for (x <- model.listSubjects) {
      graph += label(x)

      for (st <- model.listStatements(x, subClassOf, null)) {
        graph += label(x) ~> label(st.getObject.asResource)
      }
    }

    import scalax.collection.io.dot._

    println("exporting as dot")
    val root = DotRootGraph(directed = true,
      id = None,
      kvList = Seq(DotAttr("attr_1", """"one""""),
        DotAttr("attr_2", "<two>")))

    def edgeTransformer(innerEdge: Graph[String, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
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

//  println("loading triples")
//  val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)
//
//  println("converting to graph")
//  val g = GraphFactory.fromSKOS(model)
//
//  val s = "http://dbpedia.org/resource/Category:Blue_cheeses"
//  val t = "http://dbpedia.org/resource/Category:Milk"
//
//  Alg.lcsCandidates(g, s, t) map {
//    case (v, p1, p2) =>
//      val q1 = g.get(s) shortestPathTo g.get(v)
//      val q2 = g.get(t) shortestPathTo g.get(v)
//      println(f"$v - $q1 - $q2 - $p1 - $p2")
//  }


}