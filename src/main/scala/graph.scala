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

  def leastCommonSubsumer[N](g: Graph[N, GraphEdge.DiEdge], s: N, t: N) = {
    val marked = collection.mutable.Map[N, Boolean]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      println(f"marking $n")
      marked(n) = true
      VisitorReturn.Continue
    })

    var lcs: Option[N] = None

    g.get(t).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      println(f"visiting $n")
      if (marked.getOrElse(n, false)) {
        lcs = Some(n)
        println(f"selecting $n")
        VisitorReturn.Cancel
      } else VisitorReturn.Continue
    })

    lcs
  }

  def pathsTo[N](g: Graph[N, GraphEdge.DiEdge], s: N, t: N) = {
    val backlinks = collection.mutable.Map[N, List[N]]()

    println("calculating backlinks")

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = { e =>
      val from: N = e.from
      val to: N = e.to

      backlinks(to) = from :: backlinks.getOrElseUpdate(to, List.empty)
    })

    backlinks(s) = List.empty

    val memo = collection.mutable.Map[N, List[List[(N, N)]]]()

    def paths(v: N, current: List[(N, N)] = List.empty): List[List[(N, N)]] = {
      if (v == s) List(current)
      else {

        val l = for {
          u <- backlinks(v).par
          e = (u, v)
          if (!current.contains(e))
          path <- memo.getOrElseUpdate(u, paths(u, e :: current))
        } yield path

        l.toList

      }
    }

    println("building paths")
    paths(t)
  }

  def exportSubsumers[N](g: Graph[N, GraphEdge.DiEdge], s: N)(implicit m: Manifest[N]) {
    val g2 = scalax.collection.mutable.Graph[N, DiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = { e =>
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
    pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
  }


}


object GraphTest extends App {

//  println("loading triples")
//  val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)
//
//  println("converting to graph")
//  val g = GraphFactory.fromSKOS(model)
//
//  Alg.exportAsDot(g)


  val g = Graph(1 ~> 2, 1 ~> 3, 2 ~> 3, 2 ~> 4, 3 ~> 5, 4 ~> 6, 5 ~> 2, 5 ~> 6, 7 ~> 4)
  Alg.pathsTo(g, 1, 6) foreach println


}