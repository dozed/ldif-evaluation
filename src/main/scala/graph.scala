import breeze.linalg.{DenseVector, Vector}
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Aggregator, SimpleDistanceMeasure}
import de.fuberlin.wiwiss.silk.plugins.aggegrator._
import de.fuberlin.wiwiss.silk.plugins.aggegrator.AverageAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.GeometricMeanAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.MinimumAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.QuadraticMeanAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io._
import java.util.concurrent.atomic.AtomicInteger
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

object DBpediaFiles {

  val maxDistance = 77

  val categories = new File("D:/Dokumente/dbpedia2/skos_categories_en.nt")
  val categoryLabels = new File("D:/Dokumente/dbpedia2/category_labels_en.nt")

  val articleLabels = new File("D:/Dokumente/dbpedia2/labels_en.nt")
  val articleCategories = new File("D:/Dokumente/dbpedia2/article_categories_en.nt")

}

object PrefixHelper {

  val defaultPrefixes = Map(
    "category" -> "http://dbpedia.org/resource/Category:",
    "dbpedia" -> "http://dbpedia.org/resource/",
    "taaable" -> "http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A",
    "common" -> "http://example.org/common/"
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

class MutableBiMap[X, Y] {

  private val map = collection.mutable.Map[X, Y]()
  private val reverseMap = collection.mutable.Map[Y, X]()

  def update(x: X, value: Y) {
    map(x) = value
    reverseMap(value) = x
  }

  def size: Int = map.size

  def apply(key: X): Y = map(key)

  def isDefinedAt(key: X): Boolean = map.isDefinedAt(key)

  def getOrElseUpdate(key: X, value: Y): Y = {
    if (isDefinedAt(key)) this(key)
    else {
      update(key, value)
      value
    }
  }

  def inverse(key: Y): X = reverseMap(key)

  def isInRange(key: Y): Boolean = reverseMap.isDefinedAt(key)

}

case class SparseMatrix[N](default: N, dim: Int) {

  val columns = collection.mutable.Map[Int, collection.mutable.Map[Int, N]]()

  def update(i: Int, j: Int, value: N) {
    if (value != default) {
      columns(i) = columns.getOrElseUpdate(i, collection.mutable.Map[Int, N]()).updated(j, value)
    }
  }

  def apply(i: Int, j: Int): N = {
    if (columns.contains(i)) {
      if (columns(i).contains(j)) {
        columns(i)(j)
      } else default
    } else default
  }

}

case class DenseMatrix2[N: ClassTag](n: Int, m: Int) {

  val data = Array.ofDim[N](n, m)

  def update(i: Int, j: Int, value: N) {
    data(i)(j) = value
  }

  def apply(i: Int, j: Int): N = {
    data(i)(j)
  }

}

object DenseMatrix2 {
  def fill(n: Int, m: Int) = (value: Int) => {
    val mat = DenseMatrix2[Int](n, m)
    for {
      i <- 0 to n - 1
      j <- 0 to m - 1
    } mat(i, j) = value
    mat
  }
}

object GraphFactory {

  import PrefixHelper._

  val taxonomicPredicates = List(
    "http://www.w3.org/2004/02/skos/core#broader",
    "http://purl.org/dc/terms/subject",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf")

  val labelPredicates = List(
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://xmlns.com/foaf/0.1/name",
    "http://dbpedia.org/property/name")

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

  def fromQuads(in: InputStream): Graph[String, WDiEdge] = {
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

  def numericFromQuads(in: InputStream): (Graph[Int, WDiEdge], MutableBiMap[String, Int]) = {
    val g = scalax.collection.mutable.Graph[Int, WDiEdge]()
    val map = new MutableBiMap[String, Int]()
    var counter = 0

    def getOrUpdateMap0(s: String): Int = {
      if (!map.isDefinedAt(s)) {
        map(s) = counter
        counter = counter + 1
      }
      map(s)
    }

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = getOrUpdateMap0(shortenUri(p1.getSubject.stringValue))
          val o = getOrUpdateMap0(shortenUri(p1.getObject.stringValue))

          g += s ~> o % 1
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    (g, map)
  }

  def adjacencyMatrixFromQuads(in: InputStream, n: Int): DenseMatrixInt = {
    val d = new DenseMatrixInt(n, n)
    d.fill(Int.MaxValue / 2)

    println("reading triples")
    val map = new MutableBiMap[String, Int]()
    var counter = 0
    def getOrUpdateMap0(s: String): Int = {
      if (!map.isDefinedAt(s)) {
        map(s) = counter
        counter = counter + 1
      }
      map(s)
    }
    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = getOrUpdateMap0(shortenUri(p1.getSubject.stringValue))
          val o = getOrUpdateMap0(shortenUri(p1.getObject.stringValue))

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
          val o = shortenUri(p1.getObject.stringValue)
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

}

object Alg {

  // type DiHyperEdgeLikeIn[N] = DiHyperEdgeLike[N] with EdgeCopy[DiHyperEdgeLike] with EdgeIn[N,DiHyperEdgeLike]

  def merge[N](s: N, t: N) = {
    List((s ~> t % 0), (t ~> s % 0))
  }

  def weight[N](p: List[(N, Long)]): Long = p.foldLeft[Long](0)(_ + _._2)

  val inf = Int.MaxValue / 2

  def extendShortestPaths(A: DenseMatrix2[Int]): DenseMatrix2[Int] = {
    val n = A.n
    val LL = DenseMatrix2.fill(n, n)(inf)
    for {
      i <- 0 to n - 1
      j <- 0 to n - 1
      k <- 0 to n - 1
    } {
      LL(i, j) = math.min(LL(i, j), A(i, k) + A(k, j))
    }
    LL
  }

  def allPairShortestPaths(A: DenseMatrix2[Int]): DenseMatrix2[Int] = {
    val n = A.n
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

  def floydWarshall(A: DenseMatrix2[Int]): DenseMatrix2[Int] = {
    val n = A.n
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

  def test(g: Graph[String, WDiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
    //    val e1 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Blue_cheeses", 5)
    //    val e2 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Potatoes", 5)
    //    exportAsDot(e1 ++ e2)
  }

}

case class WuPalmer(g: Graph[String, WDiEdge], root: String) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    Alg.wuPalmer(g, root, value1, value2)
  }
}

case class StructuralCotopic(g: Graph[String, WDiEdge]) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    Alg.structuralCotopic(g, value1, value2)
  }
}

object TestDataset {

  import PrefixHelper._
  import GraphFactory._
  import Alg._
  import Align._

  def extractOat {
    val instances = Set(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat",
      "dbpedia:......",
      "dbpedia:----",
      "dbpedia:..._...",
      "dbpedia:-",
      "dbpedia:-_-",
      "dbpedia:--",
      "dbpedia:...---...",
      "dbpedia:-.-",
      "dbpedia:._._.",
      "dbpedia:%22_%22",
      "dbpedia:.....",
      "dbpedia:---",
      "dbpedia:...",
      "dbpedia:._.",
      "dbpedia:....",
      "dbpedia:..._---_...",
      "dbpedia:%22.%22"
    )
    val (articleTypes, categoryTypes, conceptLabels) = generateTestDataset(instances)
    writeTestDataset(new File("dataset-oat-types.nt"), articleTypes, categoryTypes, conceptLabels)
  }

  def extractCelery {
    val instances = Set(
      "dbpedia:Celery",
      "dbpedia:Cel-Ray",
      "dbpedia:Celery_salt",
      "dbpedia:Celery_Victor",
      "dbpedia:Celery_cabbage",
      "dbpedia:Celeriac",
      "dbpedia:Celebrity_(tomato)"
    )
    val (articleTypes, categoryTypes, conceptLabels) = generateTestDataset(instances)
    writeTestDataset(new File("dataset-cellery-types.nt"), articleTypes, categoryTypes, conceptLabels)
  }

  def generateTestDataset(instances: Set[String]): (Map[String, Set[String]], Set[(String, String)], Map[String, Set[String]]) = {
    println("extracting instance types")
    val articleTypes = extractArticleTypes(instances)

    println("extracting upper hierarchy")
    val categoryTypes = extractUpperCategories(articleTypes.values.flatten.toSet)

    println("extracting concept labels")
    val concepts = instances.toSet union categoryTypes.flatMap(t => Set(t._1, t._2))
    val conceptLabels = extractConceptLabels(concepts)

    (articleTypes, categoryTypes.toSet, conceptLabels)
  }

  def extractArticleTypes(subjects: Set[String]): Map[String, Set[String]] = {
    val typeMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val s = shortenUri(p1.getSubject.stringValue)
        if (subjects.contains(s)) {
          val p = p1.getPredicate.stringValue
          val o = shortenUri(p1.getObject.stringValue)
          typeMap(s) = typeMap.getOrElseUpdate(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    val in = new FileInputStream(DBpediaFiles.articleCategories)
    parser.parse(in, "http://dbpedia.org/resource")

    typeMap.toMap
  }

  def extractUpperCategories(categories: Set[String]): Set[(String, String)] = {
    val g = fromQuads(new FileInputStream(DBpediaFiles.categories))

    val conceptTypes = collection.mutable.HashSet[(String, String)]()
    val outerNodes = g.nodes.toOuterNodes
    categories foreach {
      x =>
        if (outerNodes.contains(x)) {
          g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
            e =>
              val u: String = e._1
              val v: String = e._2
              if (!conceptTypes.contains((u, v))) {
                conceptTypes += ((u, v))
              }
          })
        } else {
          println(f"  did not find category in SKOS hierarchy: $x")
        }
    }
    conceptTypes.toSet
  }

  def extractConceptLabels(concepts: Set[String]): Map[String, Set[String]] = {
    val labelMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val s = shortenUri(p1.getSubject.stringValue)
        val p = p1.getPredicate.stringValue
        if (concepts.contains(s) && labelPredicates.contains(p)) {
          val o = p1.getObject.stringValue
          labelMap(s) = labelMap.getOrElseUpdate(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    val in = new SequenceInputStream(new FileInputStream(DBpediaFiles.articleLabels),
      new FileInputStream(DBpediaFiles.categoryLabels))
    parser.parse(in, "http://dbpedia.org/resource")

    labelMap.toMap
  }

  def writeTestDataset(file: File, articleTypes: Map[String, Set[String]], categoryTypes: Set[(String, String)], conceptLabels: Map[String, Set[String]]) {
    val pw = new PrintWriter(file)

    for {
      (article, types) <- articleTypes
      articleType <- types
    } {
      val ls = f"<${fullUri(article)}> <http://purl.org/dc/terms/subject> <${fullUri(articleType)}> ."
      println(ls)
      pw.println(ls)
    }

    for {
      (cat1, cat2) <- categoryTypes
    } {
      val ls = f"<${fullUri(cat1)}> <http://www.w3.org/2004/02/skos/core#broader> <${fullUri(cat2)}> ."
      println(ls)
      pw.println(ls)
    }

    for {
      (concept, labels) <- conceptLabels
      label <- labels
    } {
      val ls = "<" + fullUri(concept) + "> <http://www.w3.org/2000/01/rdf-schema#label> \"" + label + "\"@en ."
      println(ls)
      pw.println(ls)
    }

    pw.close
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

  def nameBasedAlignment {
    val measures = List(
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaro" -> JaroDistanceMetric(),
      "jaroWinkler" -> JaroWinklerDistance(),
      "levenshtein" -> LevenshteinMetric(),
      "relaxedEquality" -> new RelaxedEqualityMetric()
    )

    def distance(sourceLabels: Set[String], targetLabels: Set[String]): (Double, String) = {
      val dists = for {
        (_, measure) <- measures
        sourceLabel <- sourceLabels
        targetLabel <- targetLabels
      } yield {
        (measure.evaluate(sourceLabel, targetLabel), targetLabel)
      }
      dists.minBy(_._1)
    }

    println("reading taaable")
    val taaableHierarchy = fromQuads(new FileInputStream("D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.nq"))

    val grainEntities = subsumedConcepts(taaableHierarchy, "taaable:Grain")
    val grainLabels = taaableLabels filterKeys grainEntities.contains

    println("reading dbpedia")
    val dbpediaLabels = labelsFromQuads(new SequenceInputStream(new FileInputStream(DBpediaFiles.articleLabels),
      new FileInputStream(DBpediaFiles.categoryLabels)))

    val thresholds = List(0.1, 0.2, 0.3, 0.05, 0.15)
    val writers = thresholds map (t => new PrintWriter(f"grain-dbpedia-concepts-t.txt"))

    for {
      (source, sourceLabels) <- grainLabels.par
      (target, targetLabels) <- dbpediaLabels
    } {
      val (dist, targetLabel) = distance(sourceLabels, targetLabels)
      if (dist < 0.1) println(f"$sourceLabels - $targetLabel - $dist")
      for {
        (threshold, pw) <- thresholds zip writers
        if (dist < threshold)
      } {
        pw.println(f"$source - $target - $sourceLabels - $targetLabels - $dist")
      }
    }

    writers foreach (_.close)
  }

  def testOat {
    val instances = List(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat",
      "dbpedia:......",
      "dbpedia:----",
      "dbpedia:..._...",
      "dbpedia:-",
      "dbpedia:-_-",
      "dbpedia:--",
      "dbpedia:...---...",
      "dbpedia:-.-",
      "dbpedia:._._.",
      "dbpedia:%22_%22",
      "dbpedia:.....",
      "dbpedia:---",
      "dbpedia:...",
      "dbpedia:._.",
      "dbpedia:....",
      "dbpedia:..._---_...",
      "dbpedia:%22.%22"
    )

    val dbpediaInstances = List(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat"
    )

    val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

    val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))

    val taaableInstances = subsumedLeafs(taaableHierarchy, "taaable:Grain")

    val g = taaableHierarchy ++ dbpediaHierarchy ++
      merge("taaable:Food", "category:Food_and_drink") +
      ("category:Food_and_drink" ~> "common:Root" % 1) +
      ("taaable:Food" ~> "common:Root" % 1)

    val containedDbpediaInstances = dbpediaInstances filter (i => g.nodes.toOuterNodes.contains(i))

    val nameBasedMeasures = List(
      "relaxedEquality" -> new RelaxedEqualityMetric(),
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaroWinkler" -> JaroWinklerDistance(),
      "jaro" -> JaroDistanceMetric(),
      "levenshtein" -> LevenshteinMetric()
    )

    val structuralMeasures = List(
      "structuralCotopic" -> StructuralCotopic(g),
      "wuPalmer" -> WuPalmer(g, "common:Root")
    )

    val e1 = "taaable:Oat"
    containedDbpediaInstances.par map {
      e2 =>
        val sb = new StringBuilder
        sb ++= e2 + ": "

        nameBasedMeasures.map(_._2) foreach {
          m =>
            val dists = for {
              l1 <- taaableLabels(e1)
              l2 <- dbpediaLabels(e2)
            } yield m.evaluate(l1, l2)
            val d = dists.min
            sb ++= f"$d%1.3f "
        }

        structuralMeasures.map(_._2) foreach {
          m =>
            val d = m.evaluate(e1, e2)
            sb ++= f"$d%1.3f "
        }

        sb.toString
    } foreach println
  }

  def extractGrains {
    val instances = fromLst(new File("ldif-taaable/grain/align-grain-name-1.lst")).right
    val (articleTypes, categoryTypes, conceptLabels) = generateTestDataset(instances)
    writeTestDataset(new File("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"), articleTypes, categoryTypes, conceptLabels)
  }

  def matchGrains {

    // sources
    val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

    val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

    val g = taaableHierarchy ++ dbpediaHierarchy ++
      merge("taaable:Food", "category:Food_and_drink") +
      ("category:Food_and_drink" ~> "common:Root" % 1) +
      ("taaable:Food" ~> "common:Root" % 1)

    // measures
    val nameBasedMeasures = List(
      "relaxedEquality" -> new RelaxedEqualityMetric(),
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaroWinkler" -> JaroWinklerDistance(),
      "jaro" -> JaroDistanceMetric(),
      "levenshtein" -> LevenshteinMetric()
    )

    val structuralMeasures = List(
      "structuralCotopic" -> StructuralCotopic(g),
      "wuPalmer" -> WuPalmer(g, "common:Root")
    )

    // candidates
    val taaableInstances = subsumedConcepts(taaableHierarchy, "taaable:Grain")
    val dbpediaInstances = dbpediaHierarchy.nodes.toOuterNodes.toSet[String] intersect dbpediaLabels.keys.toSet

    // 1. calculates distances
    // 2. aggregates distances (min)
    // 3. filters matches (threshold)
    def nameBasedMatchings(e1: String): Set[Matching] = {
      val cands = for {
        e2: String <- dbpediaHierarchy.nodes.toOuterNodes // dbpediaLabels.keys doesnt work here
        if dbpediaLabels.contains(e2)
      } yield {
        val nameBasedDists = for {
          label1 <- taaableLabels(e1)
          label2 <- dbpediaLabels(e2)
          (_, measure) <- nameBasedMeasures
        } yield {
          measure.evaluate(label1, label2)
        }

        if (nameBasedDists.size > 0) {
          val dist = nameBasedDists.min
          if (dist < 0.1) Some(Matching(e1, e2, dist))
          else None
        } else None
      }

      cands.flatten.toSet
    }

    val structuralMeasure = WuPalmer(g, "common:Root")

    def matching(e1: String, e2: String): String = {
      val nameDists = for {
        (_, measure) <- nameBasedMeasures
      } yield {
        (for {
          l1 <- taaableLabels(e1)
          l2 <- dbpediaLabels(e2)
        } yield measure.evaluate(l1, l2)) min
      }

      val sb = new StringBuilder()
      sb.append(f"$e1,$e2,")

      val lcs1 = lcs(g, e1, e2)

      lcs1 match {
        case None => sb.append(",")
        case Some((l, p1, p2)) => sb.append(f"$l,")
      }

      nameDists foreach (d => sb.append(f"$d,"))

      if (lcs1.isDefined) {
        val (l, p1, p2) = lcs1.get
        shortestPath(g, l, "common:Root") match {
          case Some(p) =>
            val w1 = weight(p1)
            val w2 = weight(p2)
            val wl = weight(p)

            // wu palmer
            val d1 = 2.0 * wl / (w1 + w2 + 2 * wl)

            // structural cotopic
            val d2 = p1.foldLeft[Long](0)(_ + _._2) + p2.foldLeft[Long](0)(_ + _._2)

            sb.append(f"$d1,$d2")
          case None =>
            sb.append(f"${Double.MaxValue},${Double.MaxValue}")
        }
      }

      sb.toString
    }

    //    val refAlign = fromLst(new File("ldif-taaable/grain/align-grain-ref.lst"))
    //    val align = Alignment(taaableInstances.par flatMap nameBasedMatchings seq)

    val pw = new PrintWriter("ldif-taaable/grain/grain-evaluation.csv")

    for {
      e1 <- taaableInstances.par
      e2 <- dbpediaInstances
      m = matching(e1, e2)
    } {
      println(m)
      pw.println(m)
    }

    pw.close


  }

  def evaluateGrains {

    case class Similarities(e1: String, e2: String, lcs: String, sim: Vector[Double]) {
      def zipWithWeights(weights: Vector[Int]): List[(Int, Double)] = {
        val wl = weights.toArray
        val s = new collection.mutable.ArrayBuffer[(Int, Double)]()
        for (i <- 0 to sim.size - 1) {
          if (wl(i) > 0.0) s += ((wl(i), sim(i)))
        }
        s.toList
      }
    }

    case class DoubleRange(from: Double, to: Double) {
      def contains(x: Double) = if (from <= x) x <= to else false
    }

    def toDouble(s: String): Double = s.replaceAll(",", ".") match {
      case "inf" => Double.MaxValue
      case x => x.toDouble
    }

    def toCSV(m: Similarities): String = {
      f"${m.e1};${m.e2};${m.lcs};${m.sim.toArray.mkString(";")}"
    }

    def product2csv(p: Product): String = p.productIterator map {
      case d: Double => f"$d%.2f".replaceAll(",", ".")
      case x => x
    } mkString(",")

    val similarities = io.Source.fromFile("grain-evaluation.csv").getLines.toList.map { l =>
      val ls = l.split(";")
      val nb = ls.slice(3, 11).toList map toDouble
      Similarities(ls(0), ls(1), ls(2), DenseVector(nb:_*))
    }

    // - outliers which cant be discriminated by a structural measure
    // - precision and recall resulting from different aggregations
    
    def toAlignment(agg: Aggregator, weights: Vector[Int], xs: List[Similarities], t: Double): Alignment = {
      val matchings = for {
        x <- xs
        sim <- agg.evaluate(x.zipWithWeights(weights))
        if (sim < t)
      } yield {
        Matching(x.e1, x.e2, sim)
      }
      Alignment(matchings.toSet)
    }

    val reference: Alignment = fromLst(new File("ldif-taaable/grain/align-grain-ref.lst"))

    // indices...
    // rel-eq	substr	qgrams	jaroWink	jaro	levensht	wuPalm	strCotopic


    def stats(agg: Aggregator, weights: Vector[Int]): Seq[(Double, Int, Int, Int, Double, Double, Double, Double)] = {
      for {
        t <- 0.001 to 0.7 by 0.01
      } yield {
        val result = toAlignment(agg, weights,  similarities, t)

        val tp = result.matchings filter reference.contains size
        val fp = result.matchings.size - tp
        val fn = reference.matchings filterNot result.contains size

        val tpa = if (result.matchings.size > 0) {
          tp.toDouble / result.matchings.size
        } else 0.0

        val tpr = if (result.matchings.size > 0) {
          tp.toDouble / reference.matchings.size
        } else 0.0

        val fm = if (tpa+tpr > 0) 2*tpa*tpr/(tpa+tpr) else 0.0
        val f2 = if (tpa+tpr > 0) 3*tpa*tpr/(3*tpa+tpr) else 0.0

        (t, tp, fp, fn, tpa, tpr, fm, f2)
      }
    }

    val A = Map(
      "min" -> MinimumAggregator(),
      "max" -> MaximumAggregator(),
      "avg" -> AverageAggregator(),
      "geoMean" -> GeometricMeanAggregator(),
      "quadMean" -> QuadraticMeanAggregator())

    val Slabels = List("req", "sub", "qgr", "jw", "ja", "lev", "wup", "sct")

    def labelWeights(weights: Vector[Int]): String = {
      (for {
        (w, i) <- weights.toArray.zipWithIndex
        if (w > 0)
      } yield f"${Slabels(i)}($w)") mkString("+")
    }

    val S0 = List(
      DenseVector(1, 0, 0, 0, 0, 0, 0, 0), // simple measures
      DenseVector(0, 1, 0, 0, 0, 0, 0, 0),
      DenseVector(0, 0, 1, 0, 0, 0, 0, 0),
      DenseVector(0, 0, 0, 1, 0, 0, 0, 0),
      DenseVector(0, 0, 0, 0, 1, 0, 0, 0),
      DenseVector(0, 0, 0, 0, 0, 1, 0, 0),
      DenseVector(0, 0, 0, 0, 0, 0, 1, 0)
//      DenseVector(0, 1, 1, 1, 1, 1, 0, 0), // aggregated string measures
//      DenseVector(0, 1, 0, 1, 0, 1, 0, 0),
//      DenseVector(0, 1, 0, 0, 0, 1, 0, 0),
//      DenseVector(0, 0, 0, 1, 0, 1, 0, 0),
//      DenseVector(0, 1, 0, 0, 0, 1, 1, 0), // aggregated all measures
//      DenseVector(0, 1, 0, 1, 0, 1, 1, 0),
//      DenseVector(0, 0, 0, 1, 0, 1, 1, 0),
//      DenseVector(0, 1, 0, 1, 0, 0, 1, 0),
//      DenseVector(0, 1, 0, 0, 0, 3, 1, 0)  // non-binary weights
    )

//    val S2 = for {
//      i0 <- 0 to 5
//      i1 <- 0 to 5
//      i2 <- 0 to 5
//      //i3 <- 0 to 5
//      //i4 <- 0 to 3
//      //i5 <- 0 to 3
//      //i6 <- 0 to 3
//      l = List(0, i0, 0, 0, 0, i2, i1, 0)
//    } yield DenseVector(l:_*)

//    val S3 = for {
//      i <- 1 to 10
//      j <- 1 to 10
//    } yield DenseVector(0, 0, 0, i, 0, 0, j, 0)

    val i = new AtomicInteger(0)

    val res = for {
      s <- S0.par
      (al, a) <- A
    } yield {
      val l = labelWeights(s)
      val pw = new PrintWriter(f"ldif-taaable/grain/res-${i.incrementAndGet}.csv")
      pw.println(f"# $l-$al")
      val r = stats(a, s).toList
      r map product2csv foreach pw.println
      pw.close

      (l, al, r.map(_._7).max, i)
    }

    res.toList.sortBy(-_._3) take (100) foreach println


  }

}

object GraphTest extends App {

  import PrefixHelper._
  import GraphFactory._
  import Alg._
  import Align._
  import TestDataset._

  // extractGrains
  // matchGrains
  evaluateGrains


}