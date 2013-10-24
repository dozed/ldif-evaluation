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

  def leastCommonSubsumer(g: Graph[String, GraphEdge.DiEdge], s: String, t: String) = {
    val marked = collection.mutable.Map[String, Boolean]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      marked(n) = true
      VisitorReturn.Continue
    })

    var lcs: Option[String] = None

    g.get(t).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      if (marked.contains(n)) {
        lcs = Some(n)
        VisitorReturn.Cancel
      } else VisitorReturn.Continue
    })

    lcs
  }

  def test(g: Graph[String, GraphEdge.DiEdge]) {
    println("lcs")
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Environmental_economics", "http://dbpedia.org/resource/Category:Green_politics"))
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Green_politics", "http://dbpedia.org/resource/Category:Environmental_economics"))
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Food_politics", "http://dbpedia.org/resource/Category:Rural_society"))
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Rural_society", "http://dbpedia.org/resource/Category:Food_politics"))
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Coffee", "http://dbpedia.org/resource/Category:Blue_cheeses"))
    println(leastCommonSubsumer(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Coffee"))
  }


}


object GraphTest extends App {

  val taaableBase = "D:/Workspaces/Dev/ldif-evaluation/ldif-taaable"
  val dbpediaBase = "D:/Dokumente/dbpedia2"

  def extractHierarchy {
    val model = RDFDataMgr.loadModel(f"file:///$dbpediaBase/skos_categories_en.nt", Lang.NTRIPLES)

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

  //  extractHierarchy

  def exportAsDot {
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)
    val graph = GraphFactory.fromSKOS(model)

    def n(outer: String) = graph get outer

    val path = n("http://dbpedia.org/resource/Category:1144") shortestPathTo n("http://dbpedia.org/resource/Category:Food_and_drink")
    println(path)

    import scalax.collection.io.dot._

    val root = DotRootGraph(directed = true,
      id = None,
      kvList = Seq(DotAttr("attr_1", """"one""""),
        DotAttr("attr_2", "<two>")))

    def edgeTransformer(innerEdge: Graph[String, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    }

    val str = new Export(graph).toDot(root, edgeTransformer)
    val pw = new PrintWriter("categories.dot")
    pw.println(str)
    pw.close
  }


  println("loading triples")
  val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)

  println("converting to graph")
  val g = GraphFactory.fromSKOS(model)

  def n(outer: String) = g get outer

  println("doing path search")
  val r = g.get("http://dbpedia.org/resource/Category:Green_politics")
  val s = g.get("http://dbpedia.org/resource/Category:Environmental_economics")
  val t = g.get("http://dbpedia.org/resource/Category:Food_and_drink")


  val p1 = s pathTo t
  val p2 = s shortestPathTo t

  println(p1)
  println(p2)

  def parentNodes(s: g.NodeT) = {
    val parents = collection.mutable.ArrayBuffer[g.NodeT]()

    s.traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = { e =>
      parents += e._2
    })

    parents.toList
  }

  def leastCommonSubsumer(s: g.NodeT, t: g.NodeT) = {
    val marked = collection.mutable.Map[g.NodeT, Boolean]()

    s.traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      marked(n) = true
      VisitorReturn.Continue
    })

    var lcs: Option[g.NodeT] = None

    s.traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = { n =>
      if (marked(n)) {
        lcs = Some(n)
        VisitorReturn.Cancel
      } else VisitorReturn.Continue
    })

    lcs
  }

  println("lcs")
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Environmental_economics"), g.get("http://dbpedia.org/resource/Category:Green_politics")))
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Green_politics"), g.get("http://dbpedia.org/resource/Category:Environmental_economics")))
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Food_politics"), g.get("http://dbpedia.org/resource/Category:Rural_society")))
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Rural_society"), g.get("http://dbpedia.org/resource/Category:Food_politics")))
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Coffee"), g.get("http://dbpedia.org/resource/Category:Potatoes")))
  println(leastCommonSubsumer(g.get("http://dbpedia.org/resource/Category:Potatoes"), g.get("http://dbpedia.org/resource/Category:Coffee")))


  //  val pa1 = parentNodes(s)
//  val pa2 = parentNodes(t)
//
//  println("diff")
//  (pa1 diff pa2) foreach println
//
//  println("intersection")
//  (pa1 intersect pa2) foreach println

  //  val g = Graph(1 ~> 2, 2 ~> 3, 3 ~> 4, 3 ~> 5, 5 ~> 6, 6 ~> 7, 4 ~> 7)
  //  val p1 = g.get(1) pathTo g.get(7)
  //  val p2 = g.get(1) shortestPathTo g.get(7)
  //
  //  println(p1)
  //  println(p2)


  //  val latch = new CountDownLatch(1)
  //  latch.await

}