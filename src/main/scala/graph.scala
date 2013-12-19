import breeze.linalg.DenseMatrix
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import java.io._
import org.apache.any23.io.nquads.NQuadsParser
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

import com.hp.hpl.jena.rdf.model.{Resource, Model}

import org.openrdf.model.Statement
import org.openrdf.rio.RDFHandler

import scala.reflect.ClassTag
import scalax.collection.Graph
import scalax.collection.GraphTraversal
import scalax.collection.GraphTraversal.VisitorReturn
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.edge.WDiEdge
import scalax.collection.edge.Implicits._

object graphFactory {

  import prefixHelper._

  def from(model: Model): Graph[String, WDiEdge] = {
    val graph = scalax.collection.mutable.Graph[String, WDiEdge]()

    taxonomicPredicates map model.getProperty foreach {
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

  def fromQuads(in: InputStream): scalax.collection.mutable.Graph[String, WDiEdge] = {
    val g = scalax.collection.mutable.Graph[String, WDiEdge]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = shortenUri(p1.getSubject.stringValue)
          val o = shortenUri(p1.getObject.stringValue)
          g += s ~> o % 1
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    g
  }

  def numericFromQuads(in: InputStream): (Graph[Int, WDiEdge], Map[Int, String]) = {
    val g = scalax.collection.mutable.Graph[Int, WDiEdge]()
    val nodes = collection.mutable.Map[String, Int]()
    var counter = 0
    def nextId = {
      val id = counter
      counter = counter + 1
      id
    }

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = nodes.getOrElseUpdate(shortenUri(p1.getSubject.stringValue), nextId)
          val o = nodes.getOrElseUpdate(shortenUri(p1.getObject.stringValue), nextId)

          g += s ~> o % 1
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    (g, nodes.map(x => (x._2, x._1)).toMap)
  }

  def adjacencyMatrixFromQuads(in: InputStream, n: Int): DenseMatrixInt = {
    val d = new DenseMatrixInt(n, n)
    d.fill(Int.MaxValue / 2)

    val nodes = collection.mutable.Map[String, Int]()
    var counter = 0
    def nextId = {
      val id = counter
      counter = counter + 1
      id
    }

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = nodes.getOrElseUpdate(shortenUri(p1.getSubject.stringValue), nextId)
          val o = nodes.getOrElseUpdate(shortenUri(p1.getObject.stringValue), nextId)

          d.set(s, o, 1)
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })
    parser.parse(in, "http://dbpedia.org/resource")
    d
  }

  def labelsFromQuads(in: InputStream): Map[String, Set[String]] = {
    val labelMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (labelPredicates.contains(p)) {
          val s = shortenUri(p1.getSubject.stringValue)
          val o = p1.getObject.stringValue
          labelMap(s) = labelMap.getOrElse(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    labelMap.toMap
  }

  def labelsFromQuadsUnique(in: InputStream): Map[String, String] = {
    for {
      (key, labels) <- labelsFromQuads(in)
    } yield {
      if (labels.size != 1) throw new Error(f"More than one label for: $key")
      (key, labels.head)
    }
  }

}

object graphAlg {

  // type DiHyperEdgeLikeIn[N] = DiHyperEdgeLike[N] with EdgeCopy[DiHyperEdgeLike] with EdgeIn[N,DiHyperEdgeLike]

  def merge[N](s: N, t: N) = {
    List((s ~> t % 0), (t ~> s % 0))
  }

  def weight[N](p: List[(N, Long)]): Long = p.foldLeft[Long](0)(_ + _._2)

  val inf = Int.MaxValue / 2

  def extendShortestPaths(A: DenseMatrix[Int]): DenseMatrix[Int] = {
    val n = A.rows
    val LL = DenseMatrix.fill(n, n)(inf)
    for {
      i <- 0 to n - 1
      j <- 0 to n - 1
      k <- 0 to n - 1
    } {
      LL(i, j) = math.min(LL(i, j), A(i, k) + A(k, j))
    }
    LL
  }

  def allPairShortestPaths(A: DenseMatrix[Int]): DenseMatrix[Int] = {
    val n = A.rows
    def ld(x: Double) = math.log(x) / math.log(2)
    val m: Int = math.round(math.pow(2, math.ceil(ld(n)))).toInt

    var L = A

    for {
      i <- 1 to m
    } {
      println(f"iteration: $i / $m")
      L = extendShortestPaths(L)
    }

    L
  }

  def floydWarshall(A: DenseMatrix[Int]): DenseMatrix[Int] = {
    val n = A.rows
    for {
      k <- 0 to n - 1
      i <- 0 to n - 1
      j <- 0 to n - 1
    } {
      A(i, j) = math.min(A(i, j), A(i, k) + A(k, j))
    }
    A
  }

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
      case None => Double.MaxValue
    }
  }

  def structuralCotopicNormalized[N](g: Graph[N, WDiEdge], s: N, t: N, maxDistance: Double): Double = {
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
        case None => Double.MaxValue
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

  def leafs[N](g: Graph[N, WDiEdge]): Set[N] = {
    g.nodes.filter(_.inDegree == 0).toOuterNodes.toSet
  }

  def subsumedLeafs[N](g: Graph[N, WDiEdge], e: N): Set[N] = {
    val leafs = collection.mutable.HashSet[N]()
    g.get(e).traverse(direction = GraphTraversal.Predecessors)(nodeVisitor = {
      n =>
        if (n.inDegree == 0) leafs += n
        VisitorReturn.Continue
    })
    leafs.toSet
  }

  def subsumedConcepts[N](g: Graph[N, WDiEdge], e: N): Set[N] = {
    val concepts = collection.mutable.HashSet[N]()
    g.get(e).traverse(direction = GraphTraversal.Predecessors)(nodeVisitor = {
      n =>
        concepts += n
        VisitorReturn.Continue
    })
    concepts.toSet
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

  def graphStatistics[N](g: Graph[N, WDiEdge]) = {
    var min = 100000
    var max = 0

    val dist = collection.mutable.Map[Int, Int]()

    for {
      s <- g.nodes.par
      t <- g.nodes
      (v, p1, p2) <- lcsCandidates[N, WDiEdge](g, s, t)
    } yield {
      val len = p1.size + p2.size
      dist(len) = dist.getOrElseUpdate(len, 0) + 1
      if (len < min) {
        println(f"new minimum: $len - $v - $p1 - $p2")
        min = len
        println(dist)
      }
      if (len > max) {
        println(f"new maximum: $len - $v - $p1 - $p2")
        max = len
        println(dist)
      }
    }

    println(f"min: $min max: $max")
    println(dist)
  }

  def test(g: Graph[String, WDiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
    //    val e1 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Blue_cheeses", 5)
    //    val e2 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Potatoes", 5)
    //    exportAsDot(e1 ++ e2)
  }

}
