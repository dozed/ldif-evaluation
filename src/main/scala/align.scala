import breeze.linalg.{DenseVector, Vector}
import de.fuberlin.wiwiss.silk.linkagerule.similarity.Aggregator
import java.io.File
import org.apache.jena.riot.{Lang, RDFDataMgr}

case class Distances(e1: String, e2: String, lcs: String, sim: Vector[Double]) {
  def zipWithWeights(weights: Vector[Int]): List[(Int, Double)] = {
    val wl = weights.toArray
    val s = new collection.mutable.ArrayBuffer[(Int, Double)]()
    for (i <- 0 to sim.size - 1) {
      if (wl(i) > 0.0) s += ((wl(i), sim(i)))
    }
    s.toList
  }
}

case class AlignmentStatistics(threshold: Double, truePositives: Int, falsePositives: Int, falseNegatives: Int,
                                truePositiveAccuracy: Double, truePositiveRate: Double) {

  def tp = truePositives
  def fp = falsePositives
  def fn = falseNegatives
  def tpa = truePositiveAccuracy
  def tpr = truePositiveRate

  def f(a: Double) = if (tpa+tpr > 0) (1+a)*tpa*tpr/(a*tpa+tpr) else 0.0

  val f1 = f(1)
  val f2 = f(2)
  val f05 = f(0.5)

}

case class Matching(e1: String, e2: String, p: Double) {
  def contains(e: String) = if (e1.equals(e)) true else e2.equals(e)
  // def covers(other: Matching): Boolean = covers(other.e1, other.e2)
  def covers(f1: String, f2: String): Boolean = if (e1.equals(f1) && e2.equals(f2)) true
    else e1.equals(f2) && e2.equals(f1)
}

case class Alignment(matchings: Set[Matching]) {

  def size: Int = matchings.size
  def foreach[U](f: Matching => U): Unit = matchings.foreach(f)

  def covers(e: String): Boolean = matchings.exists(_.contains(e))
  def covers(m: Matching): Boolean = matchings.exists(_.covers(m.e1, m.e2))

  def get(e: String): Matching = opt(e).get
  def opt(e: String): Option[Matching] = all(e).headOption
  def all(e: String): List[Matching] = matchings.filter(_.contains(e)).toList.sortBy(_.p)
  def best(e: String): Matching = bestOpt(e).get
  def bestOpt(e: String): Option[Matching] = all(e).toList sortBy (_.p) headOption

  def entities: Set[String] = left ++ right
  def left: Set[String] = matchings.map(_.e1).toSet
  def right: Set[String] = matchings.map(_.e2).toSet

  // A - B: keeps only those matching pairs from A which are not in B, doesnt take prob. into account
  def subtract(other: Alignment): Alignment = {
    Alignment(matchings filterNot other.covers)
  }

  def intersect(other: Alignment): Alignment = {
    Alignment(matchings filter other.covers)
  }

  // (A u B) can contain two matchings of two entities with different probabilities
  def union(other: Alignment): Alignment = {
    Alignment(matchings union other.matchings)
  }
}

object Align extends App {

  import PrefixHelper._
  import Alg._


  // reads possibly precalculated distances from a csv file
  def toDistances(f: String): List[Distances] = {
    def toDouble(s: String): Double = s.replaceAll(",", ".") match {
      case "inf" => Double.MaxValue
      case x => x.toDouble
    }

    io.Source.fromFile(f).getLines.toList.map { l =>
      val ls = l.split(";")
      val nb = ls.slice(3, 11).toList map toDouble
      Distances(ls(0), ls(1), ls(2), DenseVector(nb:_*))
    }
  }

  // converts a list of distance vectors to an Alignment using an aggregator, weights and a threshold
  def toAlignment(xs: List[Distances], agg: Aggregator, weights: Vector[Int], t: Double): Alignment = {
    val matchings = for {
      x <- xs
      d <- agg.evaluate(x.zipWithWeights(weights))
      if (d <= t)
    } yield {
      Matching(x.e1, x.e2, d)
    }
    Alignment(matchings.toSet)
  }

  // calculates statistics over an alignment using varying thresholds
  // enables the evaluation of fixed distance vectors, aggregator and weights
  def statistics(xs: List[Distances], R: Alignment, agg: Aggregator, weights: Vector[Int]): Seq[AlignmentStatistics] = {
    for {
      t <- 0.001 to 1.0 by 0.01   // todo increase by measuring influence on fpr
    } yield {
      val A = toAlignment(xs, agg, weights, t)

      val tp = A intersect R size
      val fp = A.size - tp
      val fn = R subtract A size

      val tpa = if (A.size > 0) {
        tp.toDouble / A.size
      } else 0.0

      val tpr = if (A.size > 0) {
        tp.toDouble / R.size
      } else 0.0

      AlignmentStatistics(t, tp, fp, fn, tpa, tpr)
    }
  }


  def fromMd(md: File): Alignment = {
    val r1 = """(.+?)\s-\s(.+?)""".r

    val matchings = io.Source.fromFile(md).getLines.toList flatMap {
      case r1(a, b) => Some(Matching(shortenUri(a), shortenUri(b), 1.0))
      case _ => None
    } toSet

    Alignment(matchings)
  }

  def fromLst(lst: File): Alignment = {
    val r1 = """\(([^\s]+?)\s*?,\s*?([^\s]+)\s*?,\s*?(\d\.\d+?)\)""".r

    val matchings = io.Source.fromFile(lst).getLines.toList flatMap {
      case r1(a, b, p) => Some(Matching(a, b, p.toDouble))
      case _ => None
    } toSet

    Alignment(matchings)
  }

  def printAlignment(a: Alignment) {
    a.matchings foreach (m => println(f"(${m.e1}, ${m.e2}, ${m.p})"))
  }

  // print info about the state of an alignment
  // for example: how much percent of each partial hierarchy is covered by the reference alignment already
  def statistics {
    val maxIdx = 909

    val r2 = """(\d+?),(.+?)""".r

    // load partial alignment
    val alignment = fromMd(new File("ldif-taaable/alignment.md"))

    val alignedEntities = io.Source.fromFile("ldif-taaable/ent-taaable.lst").getLines.toList.take(maxIdx) flatMap {
      case r2(i, uri) => Some(shortenUri(uri))
      case _ => None
    }

    println(f"coverage over matched entities (< $maxIdx): ${alignment.size} / ${alignedEntities.size} - ${alignment.size.toDouble / alignedEntities.size.toDouble}")

    // statistics for leaf entities
    val taaableGraph = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.ttl", Lang.TURTLE))

    def partialHierarchyLeafCoverage(e: String) = {
      val leafEntities = subsumedLeafs(taaableGraph, e)
      val alignedLeafEntities = leafEntities intersect alignedEntities.toSet
      val matchedLeafEntities = leafEntities intersect alignment.entities
      println(f"$e\t${matchedLeafEntities.size} / ${alignedLeafEntities.size} / ${leafEntities.size}\t${matchedLeafEntities.size.toDouble / alignedLeafEntities.size.toDouble}\t${alignedLeafEntities.size.toDouble / leafEntities.size.toDouble}")
    }

    def partialHierarchyCoverage(e: String) = {
      val subsumed = subsumedConcepts(taaableGraph, e)
      val alignedSubsumed = subsumed intersect alignedEntities.toSet
      val matchedSubsumed = subsumed intersect alignment.entities
      println(f"$e\t${matchedSubsumed.size} / ${alignedSubsumed.size} / ${subsumed.size}\t${matchedSubsumed.size.toDouble / alignedSubsumed.size.toDouble}\t${alignedSubsumed.size.toDouble / subsumed.size.toDouble}")
    }

    taaableGraph.get("taaable:Food").inNeighbors foreach { n =>
      partialHierarchyLeafCoverage(n)
    }
  }

}
