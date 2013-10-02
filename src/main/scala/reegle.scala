
import de.fuberlin.wiwiss.silk.config.Dataset
import de.fuberlin.wiwiss.silk.datasource.Source
import de.fuberlin.wiwiss.silk.entity._
import de.fuberlin.wiwiss.silk.linkagerule.input._
import de.fuberlin.wiwiss.silk.linkagerule.LinkageRule
import de.fuberlin.wiwiss.silk.linkagerule.similarity.{Aggregation, DistanceMeasure, Comparison}
import de.fuberlin.wiwiss.silk.plugins.aggegrator.AverageAggregator
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.LevenshteinMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.EqualityMetric
import de.fuberlin.wiwiss.silk.util.DPair
import java.io.File
import ldif.local.SemPRecEvaluation

/**
 * Created with IntelliJ IDEA.
 * User: stefan
 * Date: 25.09.13
 * Time: 14:54
 * To change this template use File | Settings | File Templates.
 */
object RunSilk extends App with RunSilk {

  // val base = "/home/stefan/Code/diplom-code/ldif-geo"
  val base = "D:/Workspaces/Dev/ldif-evaluation"

  val sources = List(
    Source("geonames", NewFileDataSource(f"file:///$base/dump/geonames-countries.nt")),
    Source("reegle", NewFileDataSource(f"file:///$base/dump/reegle.nt")))

  val datasets = (Dataset("geonames", "a", SparqlRestriction.fromSparql("a", "?a rdf:type gn:Feature .")),
    Dataset("reegle", "b", SparqlRestriction.fromSparql("b", "?b rdf:type gn:Feature .")))

  val sw1 = List("republic", "of", "bailiwick", "the", "kingdom", "grand duchy", "territory", "and", "principality")
  val sw2 = List("republic", "of", "bailiwick")

  val transformations = List(
    ("", identity[Input] _),
    ("lc", lc),
    ("lc-sw1", lc andThen sw(sw1)),
    ("lc-sw2", lc andThen sw(sw2)),
    ("lc-soundex", lc andThen soundex),
    ("lc-nysiis", lc andThen nysiis),
    ("lc-ml", lc andThen ml),
    ("lc-ml-sw1", lc andThen sw(sw1) andThen ml),
    ("lc-ml-sw2", lc andThen sw(sw1) andThen ml)
  )

  def codeEval {
    val inputs = {
      (PathInput(path = Path.parse("?a/gn:countryCode")) :: PathInput(path = Path.parse("?b/gn:countryCode")) :: Nil)
    } map lc

    println("evaluating code")

    val rule = LinkageRule(Comparison(
      id = "codes",
      required = true,
      metric = EqualityMetric(),
      inputs = inputs
    ))

    printFalsePositivesAndNegatives(EvaluationConfig(SemPRecEvaluation,
      new File(f"$base/ldif-geo/alignment/align-ref.rdf"),
      new File(f"$base/ldif-geo/alignment/alignment.rdf"),
      new File(f"$base/ldif-geo/alignment/align-res.rdf"),
      rule))

    printToFile(new File(f"$base/evaluation/countryCode.txt")) {
      pw =>
        val config = evaluationConfig(rule)
        val (prec, recall) = evaluate(config)
        pw.println(f"$prec, $recall")
        println(f"  $$prec, $recall")
    }

  }

  def nameRule(t: Double, metric: DistanceMeasure, inputs: DPair[Input]) = {
    LinkageRule(Comparison(
      id = "codes",
      required = false,
      //    weight = 1,
      threshold = t,
      metric = metric,
      inputs = inputs
    ))
  }

  def nameEval = (label: String, metric: DistanceMeasure, transformation: InputTransformation) => {
    val inputs = {
      (PathInput(path = Path.parse("?a/gn:name")) :: PathInput(path = Path.parse("?b/gn:name")) :: Nil)
    } map transformation

    println(f"evaluating $label")

    printToFile(new File(f"$base/evaluation/$label.txt")) {
      pw =>
        for (i <- 0 to 100) {
          val t = i / 100.0
          val rule = nameRule(t, metric, inputs)
          val config = evaluationConfig(rule)
          val (prec, recall) = evaluate(config)
          pw.println(f"${1.0 - t}, $prec, $recall")
          println(f"${1.0 - t}, $prec, $recall")
        }
    }
  }

  def aggregatedEval = {
    val countryCodeInput = {
      (PathInput(path = Path.parse("?a/gn:countryCode")) :: PathInput(path = Path.parse("?b/gn:countryCode")) :: Nil)
    } map lc

    val nameInput = {
      (PathInput(path = Path.parse("?a/gn:name")) :: PathInput(path = Path.parse("?b/gn:name")) :: Nil)
    } map lc

    println("evaluating code")

    def avgRule(t: Double) = LinkageRule(
      Aggregation(
        operators = List(
          Comparison(
            metric = EqualityMetric(),
            inputs = countryCodeInput
          ),
          Comparison(
            metric = SubStringDistance(),
            threshold = t,
            inputs = nameInput
          )
        ),
        aggregator = AverageAggregator()
      )
    )

    printToFile(new File(f"$base/evaluation/aggegated-name-code.txt")) {
      pw =>
        for (i <- 0 to 100) {
          val t = i / 100.0
          val rule = avgRule(t)
          val config = evaluationConfig(rule)
          val (prec, recall) = evaluate(config)
          pw.println(f"${1.0 - t}, $prec, $recall")
          println(f"${1.0 - t}, $prec, $recall")
        }
    }
  }

  //  for {
  //    (metricLabel, metric) <- metrics
  //    (transformationLabel, transformationFunc) <- transformations
  //  } {
  //    val label = if (transformationLabel.isEmpty) metricLabel else f"$metricLabel-$transformationLabel"
  //    eval(metric, label, transformationFunc)
  //  }


  val nameEvaluations = List(
    ("qgrams-2-lc", QGramsMetric(q = 2), lc),
    ("qgrams-3-lc", QGramsMetric(q = 3), lc),
    ("qgrams-4-lc", QGramsMetric(q = 4), lc),
    ("qgrams-5-lc", QGramsMetric(q = 5), lc),
    ("qgrams-6-lc", QGramsMetric(q = 6), lc),
    ("qgrams-8-lc", QGramsMetric(q = 8), lc),
    ("qgrams-10-lc", QGramsMetric(q = 10), lc),
    ("qgrams-2", QGramsMetric(q = 2), identity[Input] _),
    ("qgrams-2-lc", QGramsMetric(q = 2), lc),
    ("qgrams-2-lc-sw1", QGramsMetric(q = 2), lc andThen sw(sw1)),
    ("qgrams-2-lc-sw2", QGramsMetric(q = 2), lc andThen sw(sw2)),
    ("qgrams-2-lc-soundex", QGramsMetric(q = 2), lc andThen soundex),
    ("qgrams-2-lc-nysiis", QGramsMetric(q = 2), lc andThen nysiis),
    ("qgrams-2-lc-metaphone", QGramsMetric(q = 2), lc andThen metaphone),
    // TODO evaluate these too
    //    ("qgrams-2-lc-ml", QGramsMetric(q = 2), lc andThen ml),
    //    ("qgrams-2-lc-ml-sw1", QGramsMetric(q = 2), lc andThen sw(sw1) andThen ml),
    //    ("qgrams-2-lc-ml-sw2", QGramsMetric(q = 2), lc andThen sw(sw1) andThen ml),
    ("substring", SubStringDistance(), identity[Input] _),
    ("substring-metaphone", SubStringDistance(), lc andThen metaphone),
    ("qgrams-4-lc", QGramsMetric(q = 4), lc),
    ("qgrams-5-lc", QGramsMetric(q = 5), lc),
    ("qgrams-8-lc", QGramsMetric(q = 8), lc),
    ("qgrams-10-lc", QGramsMetric(q = 10), lc),
    ("jaroWinkler-lc-sw1", JaroWinklerDistance(), lc andThen sw(sw1)),
    ("jaroWinkler-lc-sw2", JaroWinklerDistance(), lc andThen sw(sw2)),
    ("jaroWinkler-lc-soundex", JaroWinklerDistance(), lc andThen soundex),
    ("jaroWinkler-lc-metaphone", JaroWinklerDistance(), lc andThen metaphone),
    ("levenshtein", LevenshteinMetric(), identity[Input] _),
    ("levenshtein-lc", LevenshteinMetric(), lc),
    ("levenshtein-lc-sw1", LevenshteinMetric(), lc andThen sw(sw1)),
    ("levenshtein-lc-sw2", LevenshteinMetric(), lc andThen sw(sw2)),
    ("levenshtein-lc-soundex", LevenshteinMetric(), lc andThen soundex),
    ("levenshtein-lc-nysiis", LevenshteinMetric(), lc andThen nysiis),
    ("levenshtein-lc-metaphone", LevenshteinMetric(), lc andThen metaphone),
    ("jaro", JaroDistanceMetric(), identity[Input] _),
    ("jaro-lc", JaroDistanceMetric(), lc),
    ("jaro-lc-sw1", JaroDistanceMetric(), lc andThen sw(sw1)),
    ("jaro-lc-sw2", JaroDistanceMetric(), lc andThen sw(sw2)),
    ("jaro-lc-soundex", JaroDistanceMetric(), lc andThen soundex),
    ("jaro-lc-nysiis", JaroDistanceMetric(), lc andThen nysiis),
    ("jaro-lc-metaphone", JaroDistanceMetric(), lc andThen metaphone)
  )

  val missing = List(
    ("jaro", JaroDistanceMetric(), identity[Input] _),
    ("jaro-lc-metaphone", JaroDistanceMetric(), lc andThen metaphone),
    ("jaroWinkler-lc-sw2", JaroWinklerDistance(), lc andThen sw(sw2)),
    ("jaroWinkler-lc-soundex", JaroWinklerDistance(), lc andThen soundex),
    ("jaroWinkler-lc-metaphone", JaroWinklerDistance(), lc andThen metaphone))

  //missing.par foreach nameEval.tupled

  //codeEval

}
