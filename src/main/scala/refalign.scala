import org.apache.jena.riot.{Lang, RDFDataMgr}

object refalign extends App {

  import PrefixHelper._
  import Alg._

  val maxIdx = 909

  val r1 = """(.+?)\s-\s(.+?)""".r
  val r2 = """(\d+?),(.+?)""".r

  // load partial alignment
  val alignment = io.Source.fromFile("ldif-taaable/alignment.md").getLines.toList flatMap {
    case r1(a, b) => Some((shortenUri(a), shortenUri(b)))
    case _ => None
  } toMap

  val alignedEntities = io.Source.fromFile("ldif-taaable/ent-taaable.lst").getLines.toList.take(maxIdx) flatMap {
    case r2(i, uri) => Some(shortenUri(uri))
    case _ => None
  }

  println(f"coverage over matched entities (< $maxIdx): ${alignment.size} / ${alignedEntities.size} - ${alignment.size.toDouble / alignedEntities.size.toDouble}")

  // statistics for leaf entities
  val taaableGraph = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.ttl", Lang.TURTLE))

  def partialHierarchyLeafCoverage(e: String) = {
    val leafEntities = subsumedLeafs(taaableGraph, e)
    val alignedLeafEntities = leafEntities filter alignedEntities.contains
    val matchedLeafEntities = leafEntities filter alignment.contains
    println(f"$e\t${matchedLeafEntities.size} / ${alignedLeafEntities.size} / ${leafEntities.size}\t${matchedLeafEntities.size.toDouble / alignedLeafEntities.size.toDouble}\t${alignedLeafEntities.size.toDouble / leafEntities.size.toDouble}")
  }

  taaableGraph.get("taaable:Food").inNeighbors foreach { n =>
    partialHierarchyLeafCoverage(n)
  }


}
