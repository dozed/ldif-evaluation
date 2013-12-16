import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import com.wcohen.ss._
import com.wcohen.ss.api.StringDistance
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import de.fuberlin.wiwiss.silk.plugins.aggegrator.MinimumAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.{QGramsMetric, SubStringDistance}
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.awt.BorderLayout
import java.io.{File, FileInputStream, PrintWriter}
import weka.classifiers.trees.J48
import weka.core.converters.{ArffSaver, CSVLoader}
import weka.gui.treevisualizer.{PlaceNode2, TreeVisualizer}

import scalax.collection.Graph
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._

object metrics extends App {

  import GraphFactory._
  import align._

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

  def generateTrainingData(): (Seq[Distances], Seq[Distances]) = {
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
    jf.setSize(500,400)
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

  def similarityFlooding {
    val g1 = Graph("Rolled oat" ~> "Oat", "Oat" ~> "Grain", "Grain" ~> "Food")
    val g2 = Graph("Oat" ~> "Oats", "Rolled oat" ~> "Oats", "Oats" ~> "Grains", "Grains" ~> "Foods")

    val pairs = (for {
      e1 <- g1.nodes
      e2 <- g2.nodes
    } yield (e1.value, e2.value)) toList

    // build propagation graph
    val pcg = scalax.collection.mutable.Graph[Int, DiEdge]()

    // add pair nodes
    for {
      i <- 0 to pairs.size - 1
    } pcg += i

    // add edges
    for {
      ((e1, e2), i1) <- pairs.zipWithIndex
      out1 <- g1.get(e1).outNeighbors
      out2 <- g2.get(e2).outNeighbors
    } {
      val i2 = pairs.indexOf((out1, out2))
      pcg += i1 ~> i2
    }

    def toDot {
      println("graph graphname {")
      for {
        ((e1, e2), i) <- pairs.zipWithIndex
      } {
        println("    " + i + " [label=\"(" + e1 + ", " + e2 + ")\"];")
      }
      for {
        p <- pcg.edges
      } {
        System.out.println(f"    ${p.from} -> ${p.to};")
      }
      println("}")
    }

    // build transition matrix
    val T = DenseMatrix.zeros[Double](pairs.size, pairs.size)

    for {
      i <- 0 to pcg.nodes.size - 1
    } yield {
      val in = pcg.get(i).inDegree
      val out = pcg.get(i).outDegree

      for {
        j <- 0 to pcg.nodes.size - 1
      } {
        // try alternative weighting strategies
        if (pcg.get(i).diSuccessors.contains(pcg.get(j))) {
          T(i, j) = 1.0 / out
        } else if (pcg.get(i).diPredecessors.contains(pcg.get(j))) {
          T(i, j) = 1.0 / in
        } else {
          T(i, j) = 0
        }
      }
    }

    // build initial alignment
    val isub = new ISub()
    val sim = for {
      (e1, e2) <- pairs
    } yield {
      math.max(isub.score(e1, e2, false), 0.001)
    }
    val s0 = DenseVector[Double](sim:_*)


    // iteration
    def p(si: DenseVector[Double]): DenseVector[Double] = {
      val v = s0 + si + T * (s0 + si)
      val z = max(v)
      v / z
    }

    def res(s1: DenseVector[Double], s2: DenseVector[Double]): Double = {
      norm(s1 - s2)
    }

    val r = (1 to 10).foldLeft(s0) { case (sp, i) =>
      val sn = p(sp)
      println("vec: " + sn)
      println("res: " + res(sp, sn))
      sn
    }

    (pairs zip r.toArray) sortBy (-_._2) foreach println


    // debug
    println("")
    for (i <- 0 to T.rows - 1) println(T(i, ::).toDenseVector)
    println("")
    println(s0)
    println("")
    val (er, ei, ev) = eig(T)
    for (i <- 1 to ev.cols - 1) println(ev(::, i).toDenseVector)

  }


//  val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))
//  println(dbpediaLabels("dbpedia:Abondance_cheese"))

  val s = new ISub
  println(s.score("Rolled oat", "Oats", false))
  println(s.score("Oats", "Rolled oat", false))

  println(SubStringDistance().evaluate("Oats", "Rolled oat"))
  println(SubStringDistance().evaluate("Rolled oat", "Oat"))

  similarityFlooding


  //  println("writing filtered training data")
  //  val (tp, fp) = generateTrainingData()

  // selection of measures and training data
  //   - good measure => good tpr/tpa ?
  //   - different measures => different tp set overlap
  //   - good training data => ?
  // aggregation
  //   - min/mean vs. decision tree

}
