import breeze.linalg.DenseMatrix
import de.fuberlin.wiwiss.silk.entity.Entity
import de.fuberlin.wiwiss.silk.linkagerule.similarity.DistanceMeasure
import java.io.File
import scala.collection.mutable.ArrayBuffer

case class SimEdge(from: Int, to: Int, sim: List[(Double, Int)])


// format: (i, j, d)
// example: (219,1166,0.4137254901960784),(219,22389,0.33333333333333337),(219,13942,0.27987220447284344),(219,13411,0.0)
object SparseDistanceMatrixIO {

  def writeSparseDistanceMatrix(entities: (Iterable[Entity], Iterable[Entity]), t: Double, normalize: String => Option[String] = s => Some(s)) = (file: File, measure: DistanceMeasure) => {
    var k = 0
    val (source, target) = entities
    val pw = new java.io.PrintWriter(file)

    for ((e1, i) <- source.zipWithIndex.par) {
      val dists = for {
        (e2, j) <- target.zipWithIndex
      } yield {
        k += 1
        if (k % 100000 == 0) println(k)

        val v1 = e1.values.flatten flatMap (s => normalize(s))
        val v2 = e2.values.flatten flatMap (s => normalize(s))
        val m = measure(v1, v2)

        if (m < t) {
          Some((i, j, m))
        } else {
          None
        }
      }

      val l = dists.flatten
      pw.println(l.mkString(","))
    }

    pw.close
  }

  def writeEdgeList(entities: (Iterable[Entity], Iterable[Entity]), t: Double, normalize: String => Option[String] = s => Some(s), file: File, measures: List[DistanceMeasure]) {

    def distanceString(i: Int, j: Int, e1: Entity, e2: Entity): Option[String] = {
      val v1 = e1.values.flatten flatMap (s => normalize(s))
      val v2 = e2.values.flatten flatMap (s => normalize(s))

      val d_ij = for {
        measure <- measures
      } yield  measure(v1, v2)

      if (d_ij.exists(_ < t)) {
        Some(f"[$i, $j, [${d_ij.mkString(",")}]]")
      } else {
        None
      }
    }

    var k = 0
    val (source, target) = entities
    val pw = new java.io.PrintWriter(file)

    for ((e1, i) <- source.zipWithIndex.par) {
      val d_i = for {
        (e2, j) <- target.zipWithIndex
      } yield {
        k += 1
        if (k % 100000 == 0) println(k)
        distanceString(i, j, e1, e2)
      }

      pw.println(f"[${d_i.flatten.mkString(",")}")
    }

    pw.close
  }

  def readSparseDistanceMatrix(file: File, m: Int, n: Int): DenseMatrix[Double] = {
    val sim = DenseMatrix.fill(m, n)(1.0)

    for {
      line <- io.Source.fromFile(file).getLines
      if (line.size > 0)
      el <- line.split("""\),?""").map(_.substring(1).split(","))
    } {
      val (i, j, v) = (el(0).toInt, el(1).toInt, el(2).toDouble)
      sim(i, j) = v
    }

    sim
  }

  def readEdgeList(file: File): List[((Int, Int), Double)] = {
    val edges = ArrayBuffer[((Int, Int), Double)]()

    for {
      line <- io.Source.fromFile(file).getLines
      if (line.size > 0)
      el <- line.split("""\),?""").map(_.substring(1).split(","))
    } {
      val edge = ((el(0).toInt, el(1).toInt), el(2).toDouble)
      edges += edge
    }

    edges.toList
  }


  def readMergedEdgeLists(files: List[File]): List[SimEdge] = {
    val edges = for {
      (file, source) <- files.zipWithIndex
      (edge, label) <- readEdgeList(file)
    } yield (edge, (label, source))

    val grouped = for {
      ((from, to), sim) <- edges groupBy (_._1) mapValues(_.map(_._2))
    } yield SimEdge(from, to, sim)

    grouped.toList
  }

}
