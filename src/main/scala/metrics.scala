import com.wcohen.ss._
import com.wcohen.ss.api.StringDistance
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.{QGramsMetric, SubStringDistance}
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io.{PrintWriter, FileInputStream}

object metrics extends App {

  import GraphFactory._
  import analyze._



  val taaableLabels = labelsFromQuadsUnique(new FileInputStream("ldif-taaable/taaable-food.nq"))
  val dbpediaLabels = labelsFromQuadsUnique(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

  val taaableTokens = labelTokens(taaableLabels).values.flatten.toSet.toList.sorted
  val dbpediaTokens = labelTokens(dbpediaLabels).values.flatten.toSet.toList.sorted



  val pw = new PrintWriter("ldif-taaable/grain/sim-labels.csv")
  var filtered = 0L
  var total = 0L

  // val t = 0.6 // filters  0.3% ~ 1gb
  // val t = 0.5 // filters >0.5% ~ 480mb
  val t = 0.5    // filters >0.8% ~ 142mb

//  println(f"expected: ${taaableTokens.size} x ${dbpediaTokens.size}")
  println(f"expected: ${taaableLabels.size} x ${dbpediaLabels.size}")

  for {
//    t1 <- taaableTokens.par
//    t2 <- dbpediaTokens
    t1 <- taaableLabels.values.par
    t2 <- dbpediaLabels.values
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

    val d = for {
      m <- measures
    } yield {
      m match {
        case d: ScaledLevenstein => 1.0 - d.score(t1, t2)  // normalized dists
        case d: MongeElkan => 1.0 - d.score(t1, t2)
        case d: Jaro => 1.0 - d.score(t1, t2)
        case d: JaroWinkler => 1.0 - d.score(t1, t2)
        case d: StringDistance => d.score(t1, t2)
        case d: ISub => 1.0 - d.score(t1, t2, false)
        case d: SimpleDistanceMeasure => d.evaluate(t1, t2)
        case _ => throw new Error("not supported")
      }
    }

    total += 1
    if (d.drop(5).exists(_ <= t)) {
      pw.println("\"" + t1 + "\",\"" + t2 + "\"," + d.mkString(","))
    } else {
      filtered += 1
    }

    if (total % 100000 == 0) println(f"$total / $filtered")
  }

  pw.close


}
