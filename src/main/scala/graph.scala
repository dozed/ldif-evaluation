import com.hp.hpl.jena.rdf.model.{Resource, Model}
import java.io.PrintWriter
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

case class Node(id: String)

case class Edge(label: String, from: String, to: String)

class Graph {

  val edges = collection.mutable.ArrayBuffer[Edge]()
  val nodes = collection.mutable.Map[String, Node]()


  def addNode(node: Node) {
    this.nodes(node.id) = node
  }

  def addEdge(edge: Edge) {
    this.edges += edge
  }

  def outgoingNodes(node: Node) = {
    outgoingEdges(node).flatMap(e => nodes.get(e.to))
  }

  def outgoingEdges(node: Node) = {
    edges.filter(_.from.equals(node.id)).toList
  }

  def incomingNodes(node: Node) = {
    incomingEdges(node).flatMap(e => nodes.get(e.from))
  }

  def incomingEdges(node: Node) = {
    edges.filter(_.to.equals(node.id)).toList
  }

}

object Graph {

  def fromRDFS(model: Model) = {
    val graph = new Graph()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")

    for (x <- model.listSubjects) {
      val s = Node(x.getURI)
      graph.addNode(s)

      for (st <- model.listStatements(x, subClassOf, null)) {
        val e = Edge("is-a", s.id, st.getObject.asResource.getURI)
        graph.addEdge(e)
      }
    }

    graph
  }

  def fromSKOS(model: Model) = {
    val graph = new Graph()
    val broader = model.getProperty("http://www.w3.org/2004/02/skos/core#broader")

    for (x <- model.listSubjects) {
      val s = Node(x.getURI)
      graph.addNode(s)

      for (st <- model.listStatements(x, broader, null)) {
        val e = Edge("is-a", s.id, st.getObject.asResource.getURI)
        graph.addEdge(e)
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

  val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/dbpedia-foods-categories-2.nt", Lang.NTRIPLES)
  val graph = Graph.fromSKOS(model)

  def upperNodes(n: Node) {
    graph.outgoingEdges(n)

  }

  graph.incomingEdges(Node("http://dbpedia.org/resource/Category:Food_and_drink")) foreach println


  //  val latch = new CountDownLatch(1)
  //  latch.await

}