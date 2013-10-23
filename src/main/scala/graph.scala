import com.hp.hpl.jena.rdf.model.{Resource, Model}
import java.io.PrintWriter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

import scalax.collection.Graph
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

  def test {

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

  println("doing path search")
  // val s = g.get("http://dbpedia.org/resource/Category:Green_politics")
  val s = g.get("http://dbpedia.org/resource/Category:Environmental_economics")
  val t = g.get("http://dbpedia.org/resource/Category:Food_and_drink")

  val p1 = s pathTo t
  val p2 = s shortestPathTo t

  println(p1)
  println(p2)



  //  val g = Graph(1 ~> 2, 2 ~> 3, 3 ~> 4, 3 ~> 5, 5 ~> 6, 6 ~> 7, 4 ~> 7)
  //  val p1 = g.get(1) pathTo g.get(7)
  //  val p2 = g.get(1) shortestPathTo g.get(7)
  //
  //  println(p1)
  //  println(p2)


  //  val latch = new CountDownLatch(1)
  //  latch.await

}