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

    var maxLcsDist: Option[Int] = None
    val candidates = collection.mutable.ArrayBuffer[N]()

    val linksS = collection.mutable.Map[N, N]()
    val linksT = collection.mutable.Map[N, N]()

    while (!Q.isEmpty) {

      val u = Q.dequeue()

      for (v <- g.get(u).diSuccessors) {
        if (!d.contains(v) && maxLcsDist.isEmpty) {  // expand if required
          d(v) = d(u) + 1
          Q += v
        }

        // update backlinks if required
        if (fromS(u) && !linksS.contains(v)) linksS(v) = u
        if (fromT(u) && !linksT.contains(v)) linksT(v) = u

        // propagate reachability
        if (!fromS.contains(v)) fromS(v) = fromS(u)
        if (!fromT.contains(v)) fromT(v) = fromT(u)

        // reachable from s and t => is a candidate for lcs
        if (fromS.contains(v) && fromT.contains(u)) {

          // set maximum lcs distance
          if (maxLcsDist.isEmpty) maxLcsDist = Some(d(v))

          if (d.contains(v) && d(v) <= maxLcsDist.get) candidates += v
        }
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
    candidates.toSet[N].toList map { v =>
      (v, backtrackPath(s, v, linksS.toMap), backtrackPath(t, v, linksT.toMap))
    }
  }

  def pathsTo[N](g: Graph[N, GraphEdge.DiEdge], s: N, t: N) = {
    val backlinks = collection.mutable.Map[N, List[N]]()

    println("calculating backlinks")
    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e =>
        val from: N = e.from
        val to: N = e.to

        backlinks(to) = from :: backlinks.getOrElseUpdate(to, List.empty)
    })

    backlinks(s) = List.empty

    var l: Long = 0L

    def paths(v: N, current: List[(N, N)] = List.empty) {
      if (l % 10000 == 0) println(f"$l: $current")
      l += 1
      if (v == s) {
        println(current)
      } else {
        for {
          u <- backlinks(v).par
          e = (u, v)
          if (!current.contains(e))
        } paths(u, e :: current)
      }
    }

    println("building paths")
    paths(t)
  }

  def exportSubsumers[N](g: Graph[N, GraphEdge.DiEdge], s: N)(implicit m: Manifest[N]) {
    val g2 = scalax.collection.mutable.Graph[N, DiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e =>
        g2 += e
    })

    exportAsDot(g)
  }

  def exportAsDot[N](g: Graph[N, GraphEdge.DiEdge]) {
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

  def test(g: Graph[String, GraphEdge.DiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
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

  println("loading triples")
  val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)

  println("converting to graph")
  val g = GraphFactory.fromSKOS(model)

  println(Alg.lcsCandidates(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Potatoes"))


}