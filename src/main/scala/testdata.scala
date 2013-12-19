import java.io.{PrintWriter, SequenceInputStream, FileInputStream, File}
import org.apache.any23.io.nquads.NQuadsParser
import org.openrdf.model.Statement
import org.openrdf.rio.RDFHandler
import scalax.collection.GraphTraversal

import collection.JavaConversions._

object testData {

  import prefixHelper._
  import graphFactory._

  def generateTestDataset(instances: Set[String]): (Map[String, Set[String]], Set[(String, String)], Map[String, Set[String]]) = {
    println("extracting instance types")
    val articleTypes = extractArticleTypes(instances)

    println("extracting upper hierarchy")
    val categoryTypes = extractUpperCategories(articleTypes.values.flatten.toSet)

    println("extracting concept labels")
    val concepts = instances.toSet union categoryTypes.flatMap(t => Set(t._1, t._2))
    val conceptLabels = extractConceptLabels(concepts)

    (articleTypes, categoryTypes.toSet, conceptLabels)
  }

  def extractArticleTypes(subjects: Set[String]): Map[String, Set[String]] = {
    val typeMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val s = shortenUri(p1.getSubject.stringValue)
        if (subjects.contains(s)) {
          val p = p1.getPredicate.stringValue
          val o = shortenUri(p1.getObject.stringValue)
          typeMap(s) = typeMap.getOrElseUpdate(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    val in = new FileInputStream(DBpediaFiles.articleCategories)
    parser.parse(in, "http://dbpedia.org/resource")

    typeMap.toMap
  }

  def extractUpperCategories(categories: Set[String]): Set[(String, String)] = {
    val g = fromQuads(new FileInputStream(DBpediaFiles.categories))

    val conceptTypes = collection.mutable.HashSet[(String, String)]()
    val outerNodes = g.nodes.toOuterNodes
    categories foreach {
      x =>
        if (outerNodes.contains(x)) {
          g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
            e =>
              val u: String = e._1
              val v: String = e._2
              if (!conceptTypes.contains((u, v))) {
                conceptTypes += ((u, v))
              }
          })
        } else {
          println(f"  did not find category in SKOS hierarchy: $x")
        }
    }
    conceptTypes.toSet
  }

  def extractConceptLabels(concepts: Set[String]): Map[String, Set[String]] = {
    val labelMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val s = shortenUri(p1.getSubject.stringValue)
        val p = p1.getPredicate.stringValue
        if (concepts.contains(s) && labelPredicates.contains(p)) {
          val o = p1.getObject.stringValue
          labelMap(s) = labelMap.getOrElseUpdate(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    val in = new SequenceInputStream(new FileInputStream(DBpediaFiles.articleLabels),
      new FileInputStream(DBpediaFiles.categoryLabels))
    parser.parse(in, "http://dbpedia.org/resource")

    labelMap.toMap
  }

  def writeTestDataset(file: File, articleTypes: Map[String, Set[String]], categoryTypes: Set[(String, String)], conceptLabels: Map[String, Set[String]]) {
    val pw = new PrintWriter(file)

    for {
      (article, types) <- articleTypes
      articleType <- types
    } {
      val ls = f"<${fullUri(article)}> <http://purl.org/dc/terms/subject> <${fullUri(articleType)}> ."
      println(ls)
      pw.println(ls)
    }

    for {
      (cat1, cat2) <- categoryTypes
    } {
      val ls = f"<${fullUri(cat1)}> <http://www.w3.org/2004/02/skos/core#broader> <${fullUri(cat2)}> ."
      println(ls)
      pw.println(ls)
    }

    for {
      (concept, labels) <- conceptLabels
      label <- labels
    } {
      val ls = "<" + fullUri(concept) + "> <http://www.w3.org/2000/01/rdf-schema#label> \"" + label + "\"@en ."
      println(ls)
      pw.println(ls)
    }

    pw.close
  }

}
