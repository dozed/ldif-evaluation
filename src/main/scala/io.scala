import breeze.linalg.DenseMatrix
import de.fuberlin.wiwiss.silk.entity.Entity
import de.fuberlin.wiwiss.silk.linkagerule.similarity.DistanceMeasure
import java.io.File

// format: (i, j, d)
// example: (219,1166,0.4137254901960784),(219,22389,0.33333333333333337),(219,13942,0.27987220447284344),(219,13411,0.0)
trait SparseDistanceMatrixIO {

  def writeSparseDistanceMatrix(entities: (Iterable[Entity], Iterable[Entity]), t: Double) = (file: File, measure: DistanceMeasure) => {
    var k = 0
    val (source, target) = entities
    val pw = new java.io.PrintWriter(file)

    for ((e1, i) <- source.zipWithIndex.par) {
      val dists = for {
        (e2, j) <- target.zipWithIndex
      } yield {
        k += 1
        if (k % 100000 == 0) println(k)

        val m = measure(e1.values.flatten, e2.values.flatten)

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


}
