import com.wcohen.ss._
import com.wcohen.ss.api.StringDistance
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
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
        pw.println("\"" +  e1 + "\",\"" + e2 + "\"," + d.mkString(","))
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

//    val taaableTokens = taaableLabels.mapValues(_ map tokenize)
//    val dbpediaTokens = labelTokens(dbpediaLabels).values.flatten.toSet.toList.sorted

    // tokens: 1429 x 7874  ~> 10x10^6
    // labels: 2165 x 24626 ~> 50x10^6 ~ 1x10^6 cps
    // t = 0.6 , filters tokens:  0.30% ~ 1gb   , labels:
    // t = 0.5 , filters tokens: >0.65% ~ 480mb , labels: 0.70% ~ 2.5gb
    // t = 0.4 , filters tokens: >0.80% ~ 142mb , labels: 0.95% ~

    calculateSimilarities(taaableLabels, dbpediaLabels, 0.4, "ldif-taaable/grain/sim-labels-0.4.csv")
  }

//  val distances = toDistances("ldif-taaable/grain/sim-tokens-0.5.csv", containsLcs = false, separator = ',', dropFirstLine = true)
//  distances foreach { d =>
//    if (d.sim(11) == 0.0 && !d.e1.equals(d.e2)) println(d)
//  }


  val ref = fromLst("ldif-taaable/grain/align-grain-ref.lst")

  // s1,s2,nw,anw,sw,lv,ap,slv,me,ja,jw,isub,req,sub,qgr
  val dists = toDistances("ldif-taaable/grain/sim-labels-0.4.csv", containsLcs = false, separator = ',', dropFirstLine = true)
  dists filter { d => ref.covers(d.e1, d.e2) } foreach { d =>
    println(d)
  }


  // tp, fp, fn => basic string measures
  // tp, fp, fn => token-based string measures
  //   - soft-tfidf: ...
  //   - tversky
  //   - soft-jaccard
  //   - tfidf, jaccard: soft with req
  // selection of measures and training data
  //   - good measure => good tpr/tpa ?
  //   - different measures => different tp set overlap
  //   - good training data => ?
  // aggregation
  //   - min/max/avg/means
  // decision tree
  //   - manual
  //   - learned

}
