import com.hp.hpl.jena.rdf.model.{Resource, Model}
import de.fuberlin.wiwiss.silk.linkagerule.similarity.SimpleDistanceMeasure
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased._
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroDistanceMetric
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.JaroWinklerDistance
import de.fuberlin.wiwiss.silk.plugins.distance.characterbased.QGramsMetric
import de.fuberlin.wiwiss.silk.plugins.distance.equality.RelaxedEqualityMetric
import java.io._
import org.apache.any23.io.nquads.NQuadsParser
import org.apache.jena.riot.{Lang, RDFDataMgr}
import collection.JavaConversions._

import org.openrdf.model.Statement
import org.openrdf.rio.RDFHandler

import scalax.collection.Graph
import scalax.collection.GraphTraversal
import scalax.collection.GraphTraversal.VisitorReturn
import scalax.collection.GraphPredef._
import scalax.collection.GraphEdge._
import scalax.collection.edge.WDiEdge
import scalax.collection.edge.Implicits._

object DBpediaFiles {

  val maxDistance = 77

  val categories = new File("D:/Dokumente/dbpedia2/skos_categories_en.nt")
  val categoryLabels = new File("D:/Dokumente/dbpedia2/category_labels_en.nt")

  val articleLabels = new File("D:/Dokumente/dbpedia2/labels_en.nt")
  val articleCategories = new File("D:/Dokumente/dbpedia2/article_categories_en.nt")

}

object PrefixHelper {

  val defaultPrefixes = Map(
    "category" -> "http://dbpedia.org/resource/Category:",
    "dbpedia" -> "http://dbpedia.org/resource/",
    "taaable" -> "http://wikitaaable.loria.fr/index.php/Special:URIResolver/Category-3A"
  )

  def shortenUri(fullUri: String): String = {
    defaultPrefixes filter {
      case (_, uriPrefix) => fullUri.startsWith(uriPrefix)
    } headOption match {
      case Some((shortPrefix, uriPrefix)) => shortPrefix + ":" + fullUri.replaceAll(uriPrefix, "")
      case None => fullUri
    }
  }

  def fullUri(shortUri: String): String = {
    defaultPrefixes filter {
      case (shortPrefix, _) => shortUri.startsWith(shortPrefix + ":")
    } headOption match {
      case Some((shortPrefix, uriPrefix)) => uriPrefix + shortUri.replaceAll(shortPrefix + ":", "")
      case None => shortUri
    }
  }

}

object GraphFactory {

  import PrefixHelper._

  val taxonomicPredicates = List(
    "http://www.w3.org/2004/02/skos/core#broader",
    "http://purl.org/dc/terms/subject",
    "http://www.w3.org/2000/01/rdf-schema#subClassOf")

  val labelPredicates = List(
    "http://www.w3.org/2000/01/rdf-schema#label",
    "http://xmlns.com/foaf/0.1/name",
    "http://dbpedia.org/property/name")

  def from(model: Model): Graph[String, WDiEdge] = {
    val graph = scalax.collection.mutable.Graph[String, WDiEdge]()

    taxonomicPredicates map model.getProperty foreach {
      p =>
        for (st <- model.listStatements(null, p, null)) {
          val e1 = shortenUri(st.getSubject.asResource.getURI)
          val e2 = shortenUri(st.getObject.asResource.getURI)
          graph += e1 ~> e2 % 1
        }
    }

    graph
  }

  def fromWikiTaxonomy: Graph[String, DiEdge] = {
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/wikipediaOntology.ttl", Lang.TURTLE)

    val graph = scalax.collection.mutable.Graph[String, DiEdge]()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    val rdfsLabel = model.getProperty("http://www.w3.org/2000/01/rdf-schema#label")

    def label(x: Resource) = {
      model.listStatements(x, rdfsLabel, null).next().getObject.asLiteral().getString
    }

    for (x <- model.listSubjects) {
      graph += label(x)

      for (st <- model.listStatements(x, subClassOf, null)) {
        graph += label(x) ~> label(st.getObject.asResource)
      }
    }

    graph

    //    import scalax.collection.io.dot._

    //    println("exporting as dot")
    //    val root = DotRootGraph(directed = true,
    //      id = None,
    //      kvList = Seq(DotAttr("attr_1", """"one""""),
    //        DotAttr("attr_2", "<two>")))
    //
    //    def edgeTransformer(innerEdge: Graph[String, DiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
    //      val edge = innerEdge.edge
    //      Some(root, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    //    }
    //
    //    val str = new Export(graph).toDot(root, edgeTransformer)
    //    val pw = new PrintWriter("wikiTaxonomy.dot")
    //    pw.println(str)
    //    pw.close
  }

  def fromQuads(in: InputStream): Graph[String, WDiEdge] = {
    val g = scalax.collection.mutable.Graph[String, WDiEdge]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (taxonomicPredicates.contains(p)) {
          val s = shortenUri(p1.getSubject.stringValue)
          val o = shortenUri(p1.getObject.stringValue)
          g += s ~> o % 1
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    g
  }

  def labelsFromQuads(in: InputStream): Map[String, Set[String]] = {
    val labelMap = collection.mutable.Map[String, Set[String]]()

    val parser = new NQuadsParser()
    parser.setRDFHandler(new RDFHandler {
      def handleStatement(p1: Statement) {
        val p = p1.getPredicate.stringValue

        if (labelPredicates.contains(p)) {
          val s = shortenUri(p1.getSubject.stringValue)
          val o = shortenUri(p1.getObject.stringValue)
          labelMap(s) = labelMap.getOrElse(s, Set.empty) + o
        }
      }

      def handleNamespace(p1: String, p2: String) {}

      def handleComment(p1: String) {}

      def startRDF() {}

      def endRDF() {}
    })

    parser.parse(in, "http://dbpedia.org/resource")

    labelMap.toMap
  }

}

object Alg {

  // type DiHyperEdgeLikeIn[N] = DiHyperEdgeLike[N] with EdgeCopy[DiHyperEdgeLike] with EdgeIn[N,DiHyperEdgeLike]

  def merge[N](s: N, t: N) = {
    List((s ~> t % 0), (t ~> s % 0))
  }

  def weight[N](p: List[(N, Long)]): Long = p.foldLeft[Long](0)(_ + _._2)

  def shortestPath[N, E[N] <: DiHyperEdgeLikeIn[N]](g: Graph[N, E], s: N, t: N): Option[List[(N, Long)]] = {
    val (_, backlinks) = shortestPaths(g, s)
    if (backlinks.contains(t)) Some(backtrackPath0(s, t, backlinks))
    else None
  }

  def shortestPaths[N, E[N] <: DiHyperEdgeLikeIn[N]](g: Graph[N, E], s: N): (Map[N, Long], Map[N, (N, Long)]) = {
    val d = collection.mutable.Map[N, Long]()
    val Q = collection.mutable.PriorityQueue[N]()(Ordering.by(d.apply))
    val backlinks = collection.mutable.Map[N, (N, Long)]()

    Q += s
    d(s) = 0

    while (!Q.isEmpty) {
      val u = Q.dequeue

      for (e <- g.get(u).outgoing) {
        val v = e.toEdgeIn._2
        val alt = d(u) + e.weight

        if (!d.contains(v) || alt < d(v)) {
          backlinks(v) = (u, e.weight)
          d(v) = alt
          Q.remove(v)
          Q.enqueue(v)
        }
      }
    }

    (d.toMap, backlinks.toMap)
  }

  private def backtrackPath0[N](from: N, to: N, backlinks: Map[N, (N, Long)]): List[(N, Long)] = {
    def path0(v: N, path: List[(N, Long)]): List[(N, Long)] = {
      if (v == from) path
      else {
        val (u, weight) = backlinks(v)
        path0(u, (u, weight) :: path)
      }
    }
    path0(to, List.empty)
  }

  def lcsCandidates[N, E[N] <: WDiEdge[N]](g: Graph[N, E], s: N, t: N): List[(N, List[(N, Long)], List[(N, Long)])] = {
    val (d_s, backlinks_s) = shortestPaths(g, s)

    val d_t = collection.mutable.Map[N, Long](t -> 0)
    val backlinks_t = collection.mutable.Map[N, (N, Long)]()
    val candidates = collection.mutable.HashSet[N]()
    val Q = collection.mutable.PriorityQueue[N]()(Ordering.by(d_t.apply))

    Q += t

    while (!Q.isEmpty) {
      val u = Q.dequeue()

      for (e <- g.get(u).outgoing) {
        val v = e._2
        val alt = d_t(u) + e.weight

        if (!d_t.contains(v) || alt < d_t(v)) {
          d_t(v) = alt
          backlinks_t(v) = (u, e.weight)

          // dont expand if it is reachable from s => a lcs candidate
          if (d_s.contains(v)) {
            candidates += v
          } else {
            Q.remove(v)
            Q.enqueue(v)
          }
        }

      }
    }

    candidates.map {
      v =>
        (v, backtrackPath0(s, v, backlinks_s.toMap), backtrackPath0(t, v, backlinks_t.toMap))
    }.toList sortBy (l => weight(l._2) + weight(l._3))
  }

  def lcs[N](g: Graph[N, WDiEdge], s: N, t: N): Option[(N, List[(N, Long)], List[(N, Long)])] = {
    lcsCandidates(g, s, t).headOption
  }

  def structuralCotopic[N](g: Graph[N, WDiEdge], s: N, t: N): Double = {
    lcs(g, s, t) match {
      case Some((_, p1, p2)) => p1.foldLeft[Long](0)(_ + _._2) + p2.foldLeft[Long](0)(_ + _._2)
      case None => 100000
    }
  }

  def structuralCotopicNormalized[N](g: Graph[N, WDiEdge], s: N, t: N, maxDistance: Double): Double = {
    structuralCotopic(g, s, t) / maxDistance
  }

  def wuPalmer[N](g: Graph[N, WDiEdge], root: N, s: N, t: N): Double = {
    val (l, p1, p2) = lcs(g, s, t).get

    shortestPath(g, l, root) match {
      case Some(p) =>
        val w1 = weight(p1)
        val w2 = weight(p2)
        val wl = weight(p)
        2.0 * wl / (w1 + w2 + 2 * wl)
      case None => 100000
    }
  }

  def subsumers[N](g: Graph[N, WDiEdge], x: N): Set[N] = {
    val subs = new collection.mutable.HashSet[N]()
    g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(nodeVisitor = {
      n =>
        subs += n
        VisitorReturn.Continue
    })
    subs.toSet
  }

  def subsumerGraph[N](g: Graph[N, WDiEdge], x: N)(implicit m: Manifest[N]): Graph[N, WDiEdge] = {
    val g2 = scalax.collection.mutable.Graph[N, WDiEdge]()

    g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e => g2 += e
    })

    g2
  }

  def subsumedLeafs[N](g: Graph[N, WDiEdge], e: N): Set[N] = {
    val leafs = collection.mutable.HashSet[N]()
    g.get(e).traverse(direction = GraphTraversal.Predecessors)(nodeVisitor = {
      n =>
        if (n.inDegree == 0) leafs += n
        VisitorReturn.Continue
    })
    leafs.toSet
  }

  def subsumedConcepts[N](g: Graph[N, WDiEdge], e: N): Set[N] = {
    val concepts = collection.mutable.HashSet[N]()
    g.get(e).traverse(direction = GraphTraversal.Predecessors)(nodeVisitor = {
      n =>
        concepts += n
        VisitorReturn.Continue
    })
    concepts.toSet
  }

  def pathsTo[N](g: Graph[N, WDiEdge], s: N, t: N): List[List[(N, N)]] = {
    val backlinks = collection.mutable.Map[N, List[N]]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
      e =>
        val from: N = e.from
        val to: N = e.to

        backlinks(to) = from :: backlinks.getOrElseUpdate(to, List.empty)
    })

    backlinks(s) = List.empty

    val buf = collection.mutable.ArrayBuffer[List[(N, N)]]()

    def backtrackPaths(v: N, current: List[(N, N)] = List.empty) {
      if (v == s) {
        buf += current
      } else {
        for {
          u <- backlinks(v).par
          e = (u, v)
          if (!current.contains(e))
        } backtrackPaths(u, e :: current)
      }
    }

    backtrackPaths(t)

    buf.toList
  }

  def exportAsDot[N](g: Graph[N, WDiEdge]): String = {
    import scalax.collection.io.dot._

    val root = DotRootGraph(directed = true, id = None)

    def edgeTransformer(innerEdge: Graph[N, WDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString.replace("http://dbpedia.org/resource/Category:", ""), edge.to.toString.replace("http://dbpedia.org/resource/Category:", ""), Nil))
    }

    new Export(g).toDot(root, edgeTransformer)
  }

  def extractEnvironment[N](g: Graph[N, WDiEdge], s: N, maxDepth: Int)(implicit m: Manifest[N]) = {
    val g2 = scalax.collection.mutable.Graph[N, WDiEdge]()

    g.get(s).traverse(direction = GraphTraversal.Successors, breadthFirst = true, maxDepth = maxDepth)(edgeVisitor = {
      e =>
        g2 += e
    })

    g2
  }

  def test(g: Graph[String, WDiEdge]) {
    //pathsTo(g, "http://dbpedia.org/resource/Category:Blue_cheeses", "http://dbpedia.org/resource/Category:Components").take(10) foreach println
    //    val e1 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Blue_cheeses", 5)
    //    val e2 = extractEnvironment(g, "http://dbpedia.org/resource/Category:Potatoes", 5)
    //    exportAsDot(e1 ++ e2)
  }

}

case class WuPalmer(g: Graph[String, WDiEdge], root: String) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    Alg.wuPalmer(g, root, value1, value2)
  }
}

case class StructuralCotopic(g: Graph[String, WDiEdge]) extends SimpleDistanceMeasure {
  def evaluate(value1: String, value2: String, limit: Double): Double = {
    Alg.structuralCotopic(g, value1, value2)
  }
}

object TestDataset {

  import PrefixHelper._
  import GraphFactory._
  import Alg._

  def generateOat {
    val instances = List(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat",
      "dbpedia:......",
      "dbpedia:----",
      "dbpedia:..._...",
      "dbpedia:-",
      "dbpedia:-_-",
      "dbpedia:--",
      "dbpedia:...---...",
      "dbpedia:-.-",
      "dbpedia:._._.",
      "dbpedia:%22_%22",
      "dbpedia:.....",
      "dbpedia:---",
      "dbpedia:...",
      "dbpedia:._.",
      "dbpedia:....",
      "dbpedia:..._---_...",
      "dbpedia:%22.%22"
    )
    val (articleTypes, categoryTypes, conceptLabels) = generateTestDataset(instances)
    writeTestDataset(new File("dataset-oat-types.nt"), articleTypes, categoryTypes, conceptLabels)
  }

  def generateCelery {
    val instances = List(
      "dbpedia:Celery",
      "dbpedia:Cel-Ray",
      "dbpedia:Celery_salt",
      "dbpedia:Celery_Victor",
      "dbpedia:Celery_cabbage",
      "dbpedia:Celeriac",
      "dbpedia:Celebrity_(tomato)"
    )
    val (articleTypes, categoryTypes, conceptLabels) = generateTestDataset(instances)
    writeTestDataset(new File("dataset-cellery-types.nt"), articleTypes, categoryTypes, conceptLabels)
  }

  def generateTestDataset(instances: List[String]): (Map[String, Set[String]], Set[(String, String)], Map[String, Set[String]]) = {
    println("extracting instance types")
    val articleTypes = extractArticleTypes(instances)

    println("extracting upper hierarchy")
    val categoryTypes = extractUpperCategories(articleTypes.values.flatten.toSet)

    println("extracting concept labels")
    val concepts = instances.toSet union categoryTypes.flatMap(t => Set(t._1, t._2))
    val conceptLabels = extractConceptLabels(concepts)

    (articleTypes, categoryTypes.toSet, conceptLabels)
  }

  def extractArticleTypes(subjects: List[String]): Map[String, Set[String]] = {
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
    categories foreach {
      x =>
        g.get(x).traverse(direction = GraphTraversal.Successors, breadthFirst = true)(edgeVisitor = {
          e =>
            val u: String = e._1
            val v: String = e._2
            if (!conceptTypes.contains((u, v))) {
              conceptTypes += ((u, v))
            }
        })
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

  def graphStatistics[N](g: Graph[N, WDiEdge]) = {
    var min = 100000
    var max = 0

    val dist = collection.mutable.Map[Int, Int]()

    for {
      s <- g.nodes.par
      t <- g.nodes
      (v, p1, p2) <- lcsCandidates[N, WDiEdge](g, s, t)
    } yield {
      val len = p1.size + p2.size
      dist(len) = dist.getOrElseUpdate(len, 0) + 1
      if (len < min) {
        println(f"new minimum: $len - $v - $p1 - $p2")
        min = len
        println(dist)
      }
      if (len > max) {
        println(f"new maximum: $len - $v - $p1 - $p2")
        max = len
        println(dist)
      }
    }

    println(f"min: $min max: $max")
    println(dist)
  }

  def wikiTaxonomyToDot {
    println("loading triples")
    val model = RDFDataMgr.loadModel(f"file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/wikipediaOntology.ttl", Lang.TURTLE)

    val graph = scalax.collection.mutable.Graph[String, WDiEdge]()
    val subClassOf = model.getProperty("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    val rdfsLabel = model.getProperty("http://www.w3.org/2000/01/rdf-schema#label")

    def label(x: Resource) = {
      model.listStatements(x, rdfsLabel, null).next().getObject.asLiteral().getString
    }

    println("building graph")
    for (x <- model.listSubjects) {
      graph += label(x)

      for (st <- model.listStatements(x, subClassOf, null)) {
        graph += label(x) ~> label(st.getObject.asResource) % 1
      }
    }

    import scalax.collection.io.dot._

    println("exporting as dot")
    val root = DotRootGraph(directed = true,
      id = None,
      kvList = Seq(DotAttr("attr_1", """"one""""),
        DotAttr("attr_2", "<two>")))

    def edgeTransformer(innerEdge: Graph[String, WDiEdge]#EdgeT): Option[(DotGraph, DotEdgeStmt)] = {
      val edge = innerEdge.edge
      Some(root, DotEdgeStmt(edge.from.toString, edge.to.toString, Nil))
    }

    val str = new Export(graph).toDot(root, edgeTransformer)
    val pw = new PrintWriter("wikiTaxonomy.dot")
    pw.println(str)
    pw.close
  }

  def nameBasedAlignment {
    val measures = List(
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaro" -> JaroDistanceMetric(),
      "jaroWinkler" -> JaroWinklerDistance(),
      "levenshtein" -> LevenshteinMetric(),
      "relaxedEquality" -> new RelaxedEqualityMetric()
    )

    def distance(sourceLabels: Set[String], targetLabels: Set[String]): (Double, String) = {
      val dists = for {
        (_, measure) <- measures
        sourceLabel <- sourceLabels
        targetLabel <- targetLabels
      } yield {
        (measure.evaluate(sourceLabel, targetLabel), targetLabel)
      }
      dists.minBy(_._1)
    }

    println("reading taaable")
    val taaableHierarchy = fromQuads(new FileInputStream("D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.nq"))

    val grainEntities = subsumedConcepts(taaableHierarchy, "taaable:Grain")
    val grainLabels = taaableLabels filterKeys grainEntities.contains

    println("reading dbpedia")
    val dbpediaLabels = labelsFromQuads(new SequenceInputStream(new FileInputStream(DBpediaFiles.articleLabels),
      new FileInputStream(DBpediaFiles.categoryLabels)))

    val thresholds = List(0.1, 0.2, 0.3, 0.05, 0.15)
    val writers = thresholds map (t => new PrintWriter(f"grain-dbpedia-concepts-t.txt"))

    for {
      (source, sourceLabels) <- grainLabels.par
      (target, targetLabels) <- dbpediaLabels
    } {
      val (dist, targetLabel) = distance(sourceLabels, targetLabels)
      if (dist < 0.1) println(f"$sourceLabels - $targetLabel - $dist")
      for {
        (threshold, pw) <- thresholds zip writers
        if (dist < threshold)
      } {
        pw.println(f"$source - $target - $sourceLabels - $targetLabels - $dist")
      }
    }

    writers foreach (_.close)
  }

  def testOat {
    val instances = List(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat",
      "dbpedia:......",
      "dbpedia:----",
      "dbpedia:..._...",
      "dbpedia:-",
      "dbpedia:-_-",
      "dbpedia:--",
      "dbpedia:...---...",
      "dbpedia:-.-",
      "dbpedia:._._.",
      "dbpedia:%22_%22",
      "dbpedia:.....",
      "dbpedia:---",
      "dbpedia:...",
      "dbpedia:._.",
      "dbpedia:....",
      "dbpedia:..._---_...",
      "dbpedia:%22.%22"
    )

    val dbpediaInstances = List(
      "dbpedia:Oaths",
      "dbpedia:Oates",
      "dbpedia:Oaten",
      "dbpedia:Oater",
      "category:Oaths",
      "dbpedia:Oatka",
      "dbpedia:Oa",
      "dbpedia:Oyat",
      "dbpedia:Oast",
      "category:Oats",
      "dbpedia:Oats",
      "dbpedia:Oath",
      "dbpedia:OAT",
      "dbpedia:Oat"
    )

    val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

    val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))

    val taaableInstances = subsumedLeafs(taaableHierarchy, "taaable:Grain")

    val g = taaableHierarchy ++ dbpediaHierarchy ++
      merge("taaable:Food", "category:Food_and_drink") +
      ("category:Food_and_drink" ~> "common:Root" % 1) +
      ("taaable:Food" ~> "common:Root" % 1)

    val containedDbpediaInstances = dbpediaInstances filter (i => g.nodes.toOuterNodes.contains(i))

    val nameBasedMeasures = List(
      "relaxedEquality" -> new RelaxedEqualityMetric(),
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaroWinkler" -> JaroWinklerDistance(),
      "jaro" -> JaroDistanceMetric(),
      "levenshtein" -> LevenshteinMetric()
    )

    val structuralMeasures = List(
      "structuralCotopic" -> StructuralCotopic(g),
      "wuPalmer" -> WuPalmer(g, "common:Root")
    )

    val e1 = "taaable:Oat"
    containedDbpediaInstances.par map {
      e2 =>
        val sb = new StringBuilder
        sb ++= e2 + ": "

        nameBasedMeasures.map(_._2) foreach {
          m =>
            val dists = for {
              l1 <- taaableLabels(e1)
              l2 <- dbpediaLabels(e2)
            } yield m.evaluate(l1, l2)
            val d = dists.min
            sb ++= f"$d%1.3f "
        }

        structuralMeasures.map(_._2) foreach {
          m =>
            val d = m.evaluate(e1, e2)
            sb ++= f"$d%1.3f "
        }

        sb.toString
    } foreach println
  }

  def testGrains {

    // sources
    val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
    val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))

    val dbpediaHierarchy = fromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))
    val dbpediaLabels = labelsFromQuads(new FileInputStream("ldif-taaable/grain/dataset-oats-articles-categories-labels.nt"))

    val g = taaableHierarchy ++ dbpediaHierarchy ++
      merge("taaable:Food", "category:Food_and_drink") +
      ("category:Food_and_drink" ~> "common:Root" % 1) +
      ("taaable:Food" ~> "common:Root" % 1)

    // measures
    val nameBasedMeasures = List(
      "relaxedEquality" -> new RelaxedEqualityMetric(),
      "substring" -> SubStringDistance(),
      "qgrams2" -> QGramsMetric(q = 2),
      "jaroWinkler" -> JaroWinklerDistance(),
      "jaro" -> JaroDistanceMetric(),
      "levenshtein" -> LevenshteinMetric()
    )

    val structuralMeasures = List(
      "structuralCotopic" -> StructuralCotopic(g),
      "wuPalmer" -> WuPalmer(g, "common:Root")
    )

    // candidates
    val taaableInstances = subsumedLeafs(taaableHierarchy, "taaable:Grain")

    def candidates0(e1: String): Set[String] = {
      val cands = for {
        e2: String <- dbpediaHierarchy.nodes.toOuterNodes // dbpediaLabels.keys doesnt work here
      } yield {
        val nameBasedDists = for {
          label1 <- taaableLabels(e1)
          label2 <- dbpediaLabels(e2)
          (_, measure) <- nameBasedMeasures.par
        } yield {
          measure.evaluate(label1, label2)
        }

        val dist = nameBasedDists.min

        if (dist < 0.1) Some(e2)
        else None
      }

      cands.flatten.toSet[String]
    }

    val cands = candidates0("taaable:Grain")
    cands foreach println

    //
    //    val e1 = "taaable:Oat"
    //    containedDbpediaInstances.par map { e2 =>
    //      val sb = new StringBuilder
    //      sb ++= e2 + ": "
    //
    //      nameBasedMeasures.map(_._2) foreach { m =>
    //        val dists = for {
    //          l1 <- taaableLabels(e1)
    //          l2 <- dbpediaLabels(e2)
    //        } yield m.evaluate(l1, l2)
    //        val d = dists.min
    //        sb ++= f"$d%1.3f "
    //      }
    //
    //      structuralMeasures.map(_._2) foreach { m =>
    //        val d = m.evaluate(e1, e2)
    //        sb ++= f"$d%1.3f "
    //      }
    //
    //      sb.toString
    //    } foreach println
  }

}

object GraphTest extends App {

  import PrefixHelper._
  import GraphFactory._
  import Alg._
  import Align._
  import TestDataset._

  testGrains
  //  generateOat


  //  println("reading taaable")
  //  val taaableHierarchy = fromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  //  val taaableLabels = labelsFromQuads(new FileInputStream("ldif-taaable/taaable-food.nq"))
  //
  //  val grainEntities = subsumedConcepts(taaableHierarchy, "taaable:Grain")
  //  val grainLabels = taaableLabels filterKeys grainEntities.contains
  //
  //  val alignment = fromLst(new File("ldif-taaable/align-grain-ref.lst"))
  //  val (matched, notMatched) = grainEntities.partition(alignment.contains)
  //
  //  println("matched:")
  //  matched foreach (m => println(m + ": " + alignment.all(m)))
  //
  //  println("not matched:")
  //  notMatched foreach println

}