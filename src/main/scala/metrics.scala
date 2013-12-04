import java.io.FileInputStream

object metrics extends App {

  import GraphFactory._
  import analyze._

//  val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

  // weights: ic, tfidf

  val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-grain-articles-categories-labels.nt"))

  dbpediaLabels.values.flatten.par filter (_ exists isPunctuation) take (10) foreach { l =>
    val tkns = tokenize(l)
    println(f"$l -> $tkns")
  }

}
