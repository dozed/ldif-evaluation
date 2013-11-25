import java.io.File
import org.apache.jena.riot.{Lang, RDFDataMgr}

case class Matching(e1: String, e2: String, p: Double) {
  def contains(e: String) = if (e1.equals(e)) true else e2.equals(e)
  def covers(other: Matching) = if (e1.equals(other.e1) && e2.equals(other.e2)) true
    else e1.equals(other.e2) && e2.equals(other.e1)
}

case class Alignment(matchings: Set[Matching]) {
  def size: Int = matchings.size

  def contains(m: Matching): Boolean = matchings.exists(_.covers(m))
  def contains(e: String): Boolean = matchings.exists(_.contains(e))
  def get(e: String): Matching = opt(e).get
  def opt(e: String): Option[Matching] = all(e).headOption
  def all(e: String): List[Matching] = matchings.filter(_.contains(e)).toList.sortBy(_.p)
  def best(e: String): Matching = bestOpt(e).get
  def bestOpt(e: String): Option[Matching] = all(e).toList sortBy (_.p) headOption

  def entities: Set[String] = left ++ right
  def left: Set[String] = matchings.map(_.e1).toSet
  def right: Set[String] = matchings.map(_.e2).toSet
}

object Align extends App {

  import PrefixHelper._
  import Alg._

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
