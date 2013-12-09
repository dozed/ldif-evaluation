import breeze.linalg.DenseVector
import com.wcohen.ss._
import com.wcohen.ss.api.StringDistance
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import de.fuberlin.wiwiss.silk.plugins.aggegrator.MinimumAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.{QGramsMetric, SubStringDistance}
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io.{FileInputStream, PrintWriter}

object metrics extends App {

  import PrefixHelper._
  import GraphFactory._
  import Alg._
  import align._
  import analyze._
  import text._

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
      dropFirstLine = true).toList filterNot (d => blacklist.contains(d.e2))

    //  5 + 0.43: 8325 - 32
    //  7 + 0.144: 2033 - 32
    //  8 + 0.087: 1720 - 32
    //  9 + 0.165: 462 - 32
    //  11 + 0.144: 450 - 32
    //  12 + 0.5: 1261 - 32
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
    //  # dfp: 8506
    println("# dtp: " + dtp.size)
    println("# dfp: " + dfp.size)

    (dtp, dfp)
  }

  println("writing filtered training data")
  val (tp, fp) = generateTrainingData()




  //  println("writing filtered training data")
  //  val (tp, fp) = generateTrainingData()
  //  val pw = new PrintWriter("ldif-taaable/grain/sim-filtered.csv")
  //  tp map toCSV foreach pw.println
  //  fp map toCSV foreach pw.println

  // selection of measures and training data
  //   - good measure => good tpr/tpa ?
  //   - different measures => different tp set overlap
  //   - good training data => ?
  // aggregation
  //   - min/mean vs. decision tree

}
