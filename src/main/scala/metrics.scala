import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}

import com.hp.hpl.jena.rdf.model.Resource
import com.wcohen.ss._
import com.wcohen.ss.api.StringDistance

import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import de.fuberlin.wiwiss.silk.plugins.aggegrator._
import de.fuberlin.wiwiss.silk.plugins.aggegrator.AverageAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.GeometricMeanAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.MaximumAggregator
import de.fuberlin.wiwiss.silk.plugins.aggegrator.MinimumAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric

import java.awt.BorderLayout
import java.io._

import no.uib.cipr.matrix
import no.uib.cipr.matrix.io.MatrixVectorReader
import no.uib.cipr.matrix.sparse.{FlexCompRowMatrix, CompColMatrix}
import org.apache.jena.riot.{Lang, RDFDataMgr}

import weka.classifiers.trees.J48
import weka.core.converters.{ArffSaver, CSVLoader}
import weka.gui.treevisualizer.{PlaceNode2, TreeVisualizer}

import scalax.collection.edge.WDiEdge
import scalax.collection.{GraphTraversal, Graph}
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.edge.Implicits._

import graphFactory._
import graphAlg._
import align._
import prefixHelper._
import testData._

import collection.JavaConversions._

case class WuPalmer(g: Graph[String, WDiEdge], root: String) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    graphAlg.wuPalmer(g, root, value1, value2)
  }
}

case class StructuralCotopic(g: Graph[String, WDiEdge]) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    graphAlg.structuralCotopic(g, value1, value2)
  }
}

object similarityFlooding {

  // type DiHyperEdgeLikeIn[N] = DiHyperEdgeLike[N] with EdgeCopy[DiHyperEdgeLike] with EdgeIn[N,DiHyperEdgeLike]

  // dot conversion
  def printPcgDot(g: Graph[Int, DiEdge], label: Int => (String, String)) {
    println("digraph g {")
    g.nodes foreach {
      n =>
        val (l1, l2) = label(n.value)
        println("    " + n.value + " [label=\"(" + l1 + ", " + l2 + ")\"];")
    }
    g.edges foreach {
      e =>
        println(f"    ${e.from} -> ${e.to};")
    }
    println("}")
  }

  def printPgDot(g: Graph[Int, WDiEdge], label: Int => (String, String)) {
    println("digraph g {")
    g.nodes foreach {
      n =>
        val (l1, l2) = label(n.value)
        println("    " + n.value + " [label=\"(" + l1 + ", " + l2 + ")\"];")
    }
    g.edges foreach {
      e =>
        println("    " + e.from + " -> " + e.to + " [label=\"" + (e.weight / 1000.0) + "\"];")
    }
    println("}")
  }

  // convert labelled graph to unlabelled graph
  def toUnlabelledGraph[E[String] <: DiHyperEdgeLikeIn[String]](g: Graph[String, E]): (Graph[Int, DiEdge], Map[Int, String]) = {
    val indices: Map[String, Int] = g.nodes.map(_.value.toString).zipWithIndex.toMap
    val labels: Map[Int, String] = indices.map(e => (e._2, e._1))

    val edges = g.edges.map {
      e => indices(e.from) ~> indices(e.to)
    }

    (Graph(edges.toSeq: _*), labels)
  }

  // build pairwise connectivity graph
  def toPCG(g1: Graph[Int, DiEdge], g2: Graph[Int, DiEdge]): Graph[Int, DiEdge] = {
    val n = g1.nodes.size
    val m = g2.nodes.size
    def idx(x: Int, y: Int): Int = y * n + x

    val nodes: Seq[Int] = (0 to (n * m) - 1).toList
    val edges: Seq[GraphParam[Int, DiEdge]] = (for {
      x <- g1.nodes
      y <- g2.nodes
      out1 <- x.outNeighbors
      out2 <- y.outNeighbors
    } yield idx(x, y) ~> idx(out1, out2)).toSeq

    Graph[Int, DiEdge](nodes: _*) ++ edges
  }

  // build propagation graph
  def toPG(pcg: Graph[Int, DiEdge]): Graph[Int, WDiEdge] = {
    val nodes: Seq[GraphParam[Int, WDiEdge]] = pcg.nodes.map(_.value).toSeq
    val edges = (for {
      i <- pcg.nodes
    } yield {
      val e1 = for (j <- pcg.get(i).inNeighbors) yield {
        j.value ~> i.value % (1000 / j.outDegree)
      }

      val e2 = for (j <- pcg.get(i).outNeighbors) yield {
        j.value ~> i.value % (1000 / j.inDegree)
      }

      // val e3 = i.value ~> i.value % (1000 * pairSim(i.value)).toLong
      // Set(e3) ++ e1 ++ e2
      e1 ++ e2
    }).flatten.toSeq
    Graph(edges: _*) ++ nodes
  }

  def toEdgeList(pcg: Graph[Int, DiEdge]): Seq[(Int, Int, Double)] = {
    var i = 0
    val edges = collection.mutable.ArrayBuffer[(Int, Int, Double)]()
    edges.sizeHint(pcg.edges.size)

    for (e <- pcg.edges) {
      val t1 = (e.from.value.asInstanceOf[Int], e.to.value.asInstanceOf[Int], (1000 / e.from.outDegree) / 1000.0)
      val t2 = (e.to.value.asInstanceOf[Int], e.from.value.asInstanceOf[Int], (1000 / e.to.inDegree) / 1000.0)
      edges += t1
      edges += t2
      i = i + 1
      if (i % 100000 == 0) println(i)
    }

    println("sorting edges")
    edges.sortWith {
      case ((i, j, _), (x, y, _)) =>
        if (i < x) true
        else if (i == x) j <= y
        else false
    }
  }

  // build transition matrix of propagation graph
  def toTransitionMatrix(pg: Graph[Int, WDiEdge], pairSim: Int => Double): CSCMatrix[Double] = {
    val T = CSCMatrix.zeros[Double](pg.nodes.size, pg.nodes.size, pg.edges.size)
    //    val T = DenseMatrix.zeros[Double](pg.nodes.size, pg.nodes.size)
    // insert in column major order

    for (i <- 0 to pg.nodes.size - 1) {
      for ((j, w) <- pg.get(i).incoming.map(e => (e.from.value, e.weight)).toList.sortBy(_._1)) {
        T(i, j) = w / 1000.0
      }
      // T(i, i) = pairSim(i.value)
    }
    T
  }

  // writes the pg to a file
  // 1. the propgation graph to <key>.mm
  // 2. the node labels to <key>.csv
  def writePg(key: String, in1: Graph[String, WDiEdge], in2: Graph[String, WDiEdge]) {
    val (g1, nodes1) = toUnlabelledGraph(in1)
    val (g2, nodes2) = toUnlabelledGraph(in2)

    val n = g1.nodes.size
    val m = g2.nodes.size

    println("building pcg")
    val pcg = toPCG(g1, g2)

    println("building pg edge list")
    val edges = toEdgeList(pcg)

    println("writing pg edges")
    val pw = new PrintWriter(key + ".mm")
    pw.println("%%MatrixMarket matrix coordinate real general")
    pw.println("% rows columns non-zero-values")
    pw.println((n * m) + " " + (n * m) + " " + edges.size)
    pw.println("% row column value")
    edges foreach pw.println
    pw.close

    println("writing pg node information (id, concept1, concept2")
    val pw3 = new PrintWriter(key + ".csv")
    pcg.nodes foreach {
      i =>
        val (x, y) = indexes(i, n)
        pw3.println(i + ",\"" + nodes1(x) + "\",\"" + nodes2(y) + "\"")
    }
    pw3.close
  }

  // reads list of coords from matrix market file, i ~> j % weight
  def readMM(file: String): List[(Int, Int, Double)] = {
    val coord = """(\d+?) (\d+?) (\d\.\d+?)""".r

    val source = io.Source.fromFile(file)
    val coords = source.getLines.toList flatMap {
      case line if line.startsWith("%") => None
      case coord(i, j, w) => Some((i.toInt, j.toInt, w.toDouble))
      case _ => None
    }
    source.close

    coords
  }

  // reads map of pcg node to node pair from original graphs g1, g2, i -> (e1, e2)
  def readPgNodes(file: String): Map[Int, (String, String)] = {
    val nodeItem = """(\d+?),(.+?),(.+?)""".r

    val coo = io.Source.fromFile(file)
    val lines = coo.getLines
    val cnts = lines.next

    val nodes = lines.toList flatMap {
      case nodeItem(id, e1, e2) => Some((id.toInt ->(e1, e2)))
      case x =>
        println(x)
        None
    }
    coo.close

    nodes.toMap
  }

  def indexes(z: Int, n: Int) = {
    ((z % n).toInt, (z / n).toInt)
  }

  def index(x: Int, y: Int, n: Int) = {
    y * n + x
  }

  // run similarity flooding algorithm
  def run(g1: Graph[Int, DiEdge], g2: Graph[Int, DiEdge], s0: DenseVector[Double]) = {
    println("building pcg")
    val pcg = toPCG(g1, g2)
    //    writeEdgeList(pcg)
    println("building pg")
    val pg = toPG(pcg)

    println("nodes: " + pcg.nodes.size + " edges: " + pcg.edges.size)

    println("building transition matrix")
    val T = toTransitionMatrix(pg, s0.apply)

    // sfa algorithm
    def res(s1: DenseVector[Double], s2: DenseVector[Double]): Double = {
      norm(s1 - s2)
    }

    val r = (1 to 20).foldLeft(s0) {
      case (sn, i) =>

        println("-----------------------------------------------------------")
        println("iteration " + i)
        // val v = s0 + sn + T * (s0 + sn)
        // val v = sn + T * sn
        val v = s0 + sn + T * sn

        println("vec: " + v)

        // unnormalized similarities after propagation
        //        v.toArray.zipWithIndex map {
        //          case (sim, z) =>
        //            val (l1, l2) = labels(z)
        //            ((l1, l2), sim)
        //        } foreach println

        val z = max(v)
        val sn1 = v / z

        val r = res(sn, sn1)

        println("vno: " + sn1)
        println("res: " + r)

        sn1
    }

    r
  }

  def evaluate {

    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
    def label1(x: String): String = taaableLabels(x) head
    def label2(x: String): String = dbpediaLabels(x) head

    //    val in1 = Graph("Rolled oat" ~> "Oat", "Oat" ~> "Grain", "Grain" ~> "Food")
    //    val in2 = Graph("Oat" ~> "Oats", "Rolled oat" ~> "Oats", "Oats" ~> "Grains", "Grains" ~> "Foods")
    //    val in1 = Graph("Rolled oat" ~> "Oat", "Oat" ~> "Grain", "Grain" ~> "Food")
    //    val in2 = Graph("Oat" ~> "Oats", "Rolled oat" ~> "Oats", "Oats" ~> "Cereals", "Grain" ~> "Cereals", "Cereals" ~> "Grains", "Grains" ~> "Staple foods", "Staple foods" ~> "Foods")
    //    val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    //    val taaableGrains = scalax.collection.mutable.Graph[String, WDiEdge]("taaable:Grain" ~> "taaable:Food" % 1)
    //    taaableHierarchy.get("taaable:Grain").traverse(direction = GraphTraversal.Predecessors)(edgeVisitor = {
    //      e => taaableGrains += e
    //    })
    //    val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
    //    val dbpediaFiltered = dbpediaHierarchy filter taaableHierarchy.having(edge = {
    //      e => dbpediaLabels.contains(e.from.value) && dbpediaLabels.contains(e.to.value)
    //    })
    // writePg("grains-pg", taaableGrains, dbpediaFiltered)

    //    776768,taaable:Buckwheat,category:Buckethead
    //    451301,taaable:Buckwheat,category:Buckethead

    println("reading matrix")
    val M = new FlexCompRowMatrix(985216, 985216)
    val coords = readMM("ldif-taaable/grains-sfa/grains-pg.mm")
    println("read " + coords.size + " coordinates")

    coords foreach {
      case (i, j, w) => M.set(i, j, w)
    }

    // precalculate multiple initial alignments
    val nodeMap = readPgNodes("ldif-taaable/grains-sfa/grains-pg.csv")
    val s0 = new matrix.DenseVector(985216)
    val isub = new ISub()

    for {
      z <- 0 to 985216 - 1
    } {
      val (x, y) = nodeMap(z)
      val l1 = label1(x)
      val l2 = label2(y)
      s0.set(z, math.max(isub.score(l1, l2, false), 0.001))
    }

    println("multiply T * s0")

    def res(v1: matrix.Vector, v2: matrix.Vector): Double = {
      var s = 0.0
      for (i <- 0 to v1.size - 1) {
        val d = math.abs(v1.get(i) - v2.get(i))
        s += d * d
      }
      math.sqrt(s)
    }

    val sn = (1 to 20).foldLeft[matrix.Vector](s0) { case (sn, i) =>
      val st = new matrix.DenseVector(985216).add(sn).add(s0)
      val sn1 = M.mult(st, new matrix.DenseVector(985216)).add(st)

      val max = sn1.norm(matrix.Vector.Norm.Infinity)
      sn1.scale(1.0 / max)

      val r = res(s0, sn)
      println("max: " + max + " res: " + r)

      sn1
    }

    sn.iterator.map(_.get).toSeq.zipWithIndex map { case (e, i) =>
      nodeMap(i) -> e
    } sortBy (-_._2) take (1000) foreach println


    //    val nodes = readPgNodes("grains-pg.csv")
    //    println("converting to transition matrix")
    //    println("writing transition matrix")
    //    write(T)

    def write(m: Matrix[Double]) {
      import scala.pickling._
      import scala.pickling.binary._
      val s = m.pickle
      val out = new FileOutputStream("out")
      out.write(s.value)
      out.close
    }

    // build initial alignment vector
    //    def initialAlignment(sim: (String, String) => Double): DenseVector[Double] = {
    //      val s0 = DenseVector.zeros[Double](n * m)
    //      for {
    //        i <- g1.nodes
    //        j <- g2.nodes
    //      } d(index(i, j, n)) = sim(nodes1(i), nodes2(j))
    //      s0
    //    }
    //
    //    val s0 = initialAlignment(simFunc)
    //    println(s0)
    //    val r = run(g1, g2, s0)

    //  // debug
    //  val ranked = r.toArray.zipWithIndex map {
    //    case (sim, z) =>
    //      val (l1, l2) = labels(z)
    //      ((l1, l2), sim)
    //  } sortBy (-_._2)
    //
    //  println("-----------------------------------------------------------")
    //  println("ranked matches")
    //  ranked foreach println
    //
    //  println("-----------------------------------------------------------")
    //  println("selecting matches")
    //  ranked groupBy (_._1._2) foreach {
    //    case (k, xs) =>
    //      println(xs sortBy (-_._2) map (_._1) head)
    //  }

    //    println("")
    //    println(s0)
    //    println("")
    //    val (er, ei, ev) = eig(T)
    //    for (i <- 1 to ev.cols - 1) println(ev(::, i).toDenseVector)
  }
}

object metrics extends App {

  def calculateSimilarities(m1: Map[String, Set[String]], m2: Map[String, Set[String]], t: Double, out: String) {
    def distance(m: Any, s1: String, s2: String) = {
      m match {
        case d: ScaledLevenstein => 1.0 - d.score(s1, s2) // normalized dists
        case d: MongeElkan => 1.0 - d.score(s1, s2)
        case d: Jaro => 1.0 - d.score(s1, s2)
        case d: JaroWinkler => 1.0 - d.score(s1, s2)
        case d: StringDistance => d.score(s1, s2)
        case d: ISub => 1.0 - d.score(s1, s2, false)
        case d: SimpleDistanceMeasure => d.evaluate(s1, s2)
        case d: SimpleDistanceMeasure => d.evaluate(s1, s2)
        case _ => throw new Error("not supported")
      }
    }

    val pw = new PrintWriter(out)
    var filtered = 0L
    var total = 0L

    println(f"expected: ~ ${m1.size} x ${m2.size}")

    // nw, anw, sw, lv, ap, slv, me, ja, jw, isub, req, sub, qgr

    pw.println("s1,s2,nw,anw,sw,lv,ap,slv,me,ja,jw,isub,req,sub,qgr")

    for {
      (e1, xs) <- m1.par
      (e2, ys) <- m2
    } {
      val measures = List(
        new NeedlemanWunsch(),
        new ApproxNeedlemanWunsch(),
        new SmithWaterman(),
        new Levenstein(),
        new AffineGap(),
        new ScaledLevenstein(),
        new MongeElkan(),
        new Jaro(),
        new JaroWinkler(),
        new ISub(),
        new RelaxedEqualityMetric(),
        SubStringDistance(),
        QGramsMetric(q = 2)
      )

      // possibly multiple labels => choose best-match
      val d = for {
        m <- measures
      } yield {
        (for {
          x <- xs
          y <- ys
        } yield {
          distance(m, x, y)
        }) min
      }

      total += 1
      if (d.drop(5).exists(_ <= t)) {
        pw.println("\"" + e1 + "\",\"" + e2 + "\"," + d.mkString(","))
      } else {
        filtered += 1
      }

      if (total % 100000 == 0) println(f"$total / $filtered")
    }

    pw.close
  }

  def writeSimilarities {
    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

    // tokens: 1429 x 7874  ~> 10x10^6
    // labels: 2165 x 24626 ~> 50x10^6 ~ 1x10^6 cps
    // t = 0.6 , filters tokens:  0.30% ~ 1gb   , labels:
    // t = 0.5 , filters tokens: >0.65% ~ 480mb , labels: 0.70% ~ 2.5gb
    // t = 0.4 , filters tokens: >0.80% ~ 142mb , labels: 0.95% ~

    calculateSimilarities(taaableLabels, dbpediaLabels, 0.4, "ldif-taaable/grain/sim-labels-0.4.csv")
  }

  def generateTrainingData: (Seq[Distances], Seq[Distances]) = {
    println("reading ref alignment")
    val R = fromLst("ldif-taaable/grain/align-grain-ref.lst")
    val left = R.left

    // TODO check why those appear to be in low distance
    val blacklist = Set(
      "dbpedia:...",
      "dbpedia:....",
      "dbpedia:.....",
      "dbpedia:......",
      "dbpedia:..._---_...",
      "dbpedia:..._...",
      "dbpedia:._._.",
      "dbpedia:...---...",
      "dbpedia:-",
      "dbpedia:--",
      "dbpedia:---",
      "dbpedia:----",
      "dbpedia:._.",
      "dbpedia:-.-",
      "dbpedia:-_-",
      "dbpedia:%22.%22",
      "dbpedia:%22_%22"
    )

    println("reading precalculated distance vectors")
    val dists = toDistances("ldif-taaable/grain/sim-labels-0.4.csv",
      containsLcs = false,
      separator = ',',
      dropFirstLine = true).toList filter (d => left.contains(d.e1)) filterNot (d => blacklist.contains(d.e2))

    //  5 + 0.43: 1976 - 32
    //  7 + 0.144: 1330 - 32
    //  8 + 0.087: 1157 - 32
    //  9 + 0.165: 295 - 32
    //  11 + 0.144: 267 - 32
    //  12 + 0.5: 794 - 32
    val ts = List(
      (5, DenseVector(0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0), 0.430),
      (7, DenseVector(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0), 0.144),
      (8, DenseVector(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0), 0.087),
      (9, DenseVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0), 0.165),
      (11, DenseVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0), 0.144),
      (12, DenseVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1), 0.500)
    )

    val tv = DenseVector(0, 0, 0, 0, 0, 1, 0, 1, 1, 1, 0, 1, 1)

    val agg = MinimumAggregator()

    println("calculating statistics")
    ts map {
      case (i, weights, t) =>
        val A = toAlignment(dists, agg, weights, t)
        val s = statistics(A, R)
        println(f"$i + $t: ${s.fp} - ${s.tp}")
    }

    println("selecting non-trivial true positive matches")
    val dtp = dists filter {
      d => R.covers(d.e1, d.e2)
    } filterNot {
      d => d.isTrivial(tv)
    }

    println("selecting false positive matches (only a few)")
    val dfp = dists filter {
      d =>
        ts exists {
          case (i, weights, t) => agg.evaluate(d.zipWithWeights(weights)) forall (_ <= t)
        }
    } filterNot {
      d => R.covers(d.e1, d.e2)
    }

    //  # dtp: 7
    //  # dfp: 2068
    println("# dtp: " + dtp.size)
    println("# dfp: " + dfp.size)

    val pw = new PrintWriter("ldif-taaable/grain/sim-filtered.csv")
    dtp map toCSV foreach pw.println
    dfp map toCSV foreach pw.println
    pw.close

    (dtp, dfp)
  }

  def j48 {
    // save ARFF
    val loader = new CSVLoader
    loader.setSource(new File("ldif-taaable/grain/sim-filtered.csv"))
    val data = loader.getDataSet
    val saver = new ArffSaver
    saver.setInstances(data)
    saver.setFile(new File("ldif-taaable/grain/sim-filtered.arff"))
    //saver.setDestination(new File("ldif-taaable/grain/sim-filtered.arff"))
    saver.writeBatch


    val cls = new J48
    //  val data = new Instances(new FileReader("ldif-taaable/grain/sim-filtered-2.arff"))
    data.setClassIndex(3)
    cls.buildClassifier(data)

    val jf = new javax.swing.JFrame("Weka Classifier Tree Visualizer: J48")
    jf.setSize(500, 400)
    jf.getContentPane().setLayout(new BorderLayout)
    val tv = new TreeVisualizer(null, cls.graph(), new PlaceNode2())
    jf.getContentPane().add(tv, BorderLayout.CENTER)
    jf.addWindowListener(new java.awt.event.WindowAdapter() {
      override def windowClosing(e: java.awt.event.WindowEvent) {
        jf.dispose()
      }
    })

    jf.setVisible(true)
    tv.fitToScreen()
  }

  similarityFlooding.evaluate

  //  val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  //  val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  //
  //  val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
  //  val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
  //
  //  val taaableInstances = subsumedConcepts(taaableHierarchy, "taaable:Grain")
  //  val dbpediaInstances = dbpediaHierarchy.nodes.toOuterNodes.toSet[String] intersect dbpediaLabels.keys.toSet
  //
  //  val g = taaableHierarchy ++ dbpediaHierarchy ++
  //    merge("taaable:Food", "category:Food_and_drink") +
  //    ("category:Food_and_drink" ~> "common:Root" % 1) +
  //    ("taaable:Food" ~> "common:Root" % 1)

  //  println("writing filtered training data")
  //  val (tp, fp) = generateTrainingData()

  // selection of measures and training data
  //   - good measure => good tpr/tpa ?
  //   - different measures => different tp set overlap
  //   - good training data => ?
  // aggregation
  //   - min/mean vs. decision tree

}

object testDataset {

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

    def toCSV(m: Distances): String = {
      f"${m.e1};${m.e2};${m.lcs};${m.dist.toArray.mkString(";")}"
    }

    def product2csv(p: Product): String = p.productIterator map {
      case d: Double => f"$d%.2f".replaceAll(",", ".")
      case x => x
    } mkString (",")


    val distances = toDistances("grain-evaluation.csv")
    val reference: Alignment = fromLst(new File("ldif-taaable/grain/align-grain-ref.lst"))

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
      } yield f"${Slabels(i)}($w)") mkString ("+")
    }

    val S0 = List(
      //      DenseVector(1, 0, 0, 0, 0, 0, 0, 0), // simple measures
      //      DenseVector(0, 1, 0, 0, 0, 0, 0, 0),
      //      DenseVector(0, 0, 1, 0, 0, 0, 0, 0),
      //      DenseVector(0, 0, 0, 1, 0, 0, 0, 0),
      //      DenseVector(0, 0, 0, 0, 1, 0, 0, 0),
      //      DenseVector(0, 0, 0, 0, 0, 1, 0, 0),
      //      DenseVector(0, 0, 0, 0, 0, 0, 1, 0)
      //      DenseVector(0, 1, 1, 1, 1, 1, 0, 0), // aggregated string measures
      //      DenseVector(0, 1, 0, 1, 0, 1, 0, 0),
      //      DenseVector(0, 1, 0, 0, 0, 1, 0, 0),
      //      DenseVector(0, 0, 0, 1, 0, 1, 0, 0)
      DenseVector(0, 1, 0, 0, 0, 1, 1, 0), // aggregated all measures
      DenseVector(0, 1, 0, 1, 0, 1, 1, 0),
      DenseVector(0, 0, 0, 1, 0, 1, 1, 0),
      DenseVector(0, 1, 0, 1, 0, 0, 1, 0),
      DenseVector(0, 1, 0, 0, 0, 3, 1, 0) // non-binary weights
    )

    val S2 = for {
      i0 <- 0 to 1
      i1 <- 0 to 1
      i2 <- 0 to 1
      i3 <- 0 to 1
      i4 <- 0 to 1
      i5 <- 0 to 1
      //i6 <- 0 to 3
      l = List(0, i0, i1, i2, i3, i4, i5, 0)
    } yield DenseVector(l: _*)

    //    val S3 = for {
    //      i <- 1 to 10
    //      j <- 1 to 10
    //    } yield DenseVector(0, 0, 0, i, 0, 0, j, 0)

    val res = for {
      (s, i) <- S2.zipWithIndex.par
      // ((al, a), j) <- A.zipWithIndex
      // ((al, a), j) = (("min", MinimumAggregator()), 0)
      ((al, a), j) = (("geoMean", GeometricMeanAggregator()), 0)
    } yield {
      val l = labelWeights(s)
      //      val idx = i*A.size+j+1
      //      val pw = new PrintWriter(f"ldif-taaable/grain/agg-all-${idx}.csv")
      //      pw.println(f"# $l-$al")
      val r = statistics(distances, reference, a, s).toList
      //      r map product2csv foreach pw.println
      //      pw.close

      (l, al, r.map(_._2.f05).max, i)
    }

    res.toList.sortBy(-_._3) take (100) foreach println


    //    stats(MinimumAggregator(), DenseVector(0, 0, 0, 0, 0, 1, 0, 0)) foreach { case (t, tp, fp, fn, tpa, tpr, fm, f2) =>
    //      println(f"$t%.2f;$tp;$fp;$fn;$tpa%.2f;$tpr%.2f;$fm%.2f;$f2%.2f")
    //    }
  }

  def evaluateGrains2 {

    val distances = toDistances("ldif-taaable/grain/grain-evaluation.csv")

    val reference = fromLst(new File("ldif-taaable/grain/align-grain-ref.lst"))

    def approx(t: Double): Alignment = {
      //toAlignment(distances, MinimumAggregator(), DenseVector(1, 1, 1, 1, 1, 1, 0, 0), t)   // t: 0.1
      toAlignment(distances, MinimumAggregator(), DenseVector(0, 0, 0, 0, 0, 1, 0, 0), t)
    }

    val trivial = reference intersect approx(0.0)
    val hard = reference subtract approx(0.8) subtract trivial

    //    println("trivial:")
    //    trivialMatches.matchings foreach println

    //    println("non-trivial:")
    //    (reference subtract trivialMatches).matchings foreach println

    //    println("hard:")
    //    hard foreach println

    println("distances: " + distances.size)
    println("refalign: " + reference.size)
    println("trivial: " + trivial.size)
    println("hard: " + hard.size)

    var previousTpr = 0.0
    for (t <- 0.001 to 1.0 by 0.001) {
      val a = approx(t) // subtract trivial   // recall+
      val s = statistics(a, reference)
      if (s.tpr > previousTpr) {
        println(f"approx($t%.3f): tp: ${s.tp} fp: ${s.fp} tpr: ${s.tpr} tpa: ${s.tpa} ")
        previousTpr = s.tpr
      }
    }



    // List("req", "sub", "qgr", "jw", "ja", "lev", "wup", "sct")

    //    trivialMatches.matchings foreach { m =>
    //      distances.filter(s => m.covers(s.e1, s.e2))
    //    }

  }

  // removes category/relation concepts by applying a name-based and degree-based heuristic
  def cleanup(g: scalax.collection.mutable.Graph[String, WDiEdge]) {
    g.nodes filter DBpediaConceptFilter.isCategory foreach g.remove
    for (i <- 1 to 100) {
      g.nodes.filter(n => n.outDegree == 0) foreach {
        n =>
          println(f"$i: $n")
          g.remove(n)
      }
    }
  }

}