import org.apache.jena.riot.{Lang, RDFDataMgr}

import scalax.collection.Graph
import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._

import PrefixHelper._
import Alg._

class lcsTests {

  def lcs0() {
    //    val g1 = Graph(1 ~> 2, 1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 7, 7 ~> 8, 5 ~> 6, 6 ~> 4, 6 ~> 1, 6 ~> 8, 2 ~> 8)
    //    println(Alg.lcsCandidates(g1, 1, 5))
    //
    //    val g2 = Graph(1 ~> 3, 1 ~> 4, 2 ~> 5, 2 ~> 12, 3 ~> 6, 4 ~> 7, 5 ~> 8, 6 ~> 9, 7 ~> 11, 8 ~> 11, 9 ~> 12, 10 ~> 12, 11 ~> 10)
    //    println(Alg.lcsCandidates(g2, 1, 2))
    //    println(Alg.pathsTo(g2, 1, 12))
    //    println(Alg.pathsTo(g2, 2, 12))
    //
    //    val g3 = Graph(1 ~> 3, 1 ~> 4, 2 ~> 4, 3 ~> 5, 4 ~> 6, 5 ~> 7, 6 ~> 8, 7 ~> 8)
    //    val g4 = Graph(1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 5, 4 ~> 6, 6 ~> 7, 6 ~> 8, 8 ~> 9)
    //    val g5 = Graph(1 ~> 3, 2 ~> 4, 3 ~> 4, 4 ~> 5, 4 ~> 6, 6 ~> 7, 6 ~> 8, 8 ~> 9,
    //      1 ~> 20, 20 ~> 21, 21 ~> 22, 22 ~> 23, 23 ~> 24, 24 ~> 25, 25 ~> 26, 26 ~> 27, 27 ~> 28, 28 ~> 29, 29 ~> 30, 30 ~> 42,
    //      42 ~> 7, 42 ~> 8, 42 ~> 9)
    //
    //    Alg.lcsCandidates(g5, 1, 2) map {
    //      case (v, p1, p2) =>
    //        val q1 = g5.get(1) shortestPathTo g5.get(v)
    //        val q2 = g5.get(2) shortestPathTo g5.get(v)
    //        println(f"$v - $q1 - $q2 - $p1 - $p2")
    //    }
  }

  def lcs1() {

    val instances = List(
      "http://dbpedia.org/resource/Celery",
      "http://dbpedia.org/resource/Cel-Ray",
      "http://dbpedia.org/resource/Celery_salt",
      "http://dbpedia.org/resource/Celery_Victor",
      "http://dbpedia.org/resource/Celery_cabbage",
      "http://dbpedia.org/resource/Celeriac",
      "http://dbpedia.org/resource/Celebrity_(tomato)"
    )

    val taaable = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/taaable-food.ttl", Lang.TURTLE))
    val dbpedia = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/celery/test-celery.nt", Lang.NTRIPLES))

    val g = dbpedia ++ taaable ++
      merge("taaable:Food", "category:Food_and_drink") ++
      merge("taaable:Vegetable", "category:Vegetables") ++
      merge("taaable:Stalk_vegetable", "category:Stem_vegetables") ++
      merge("taaable:Leaf_vegetable", "category:Leaf_vegetables") +
      ("category:Food_and_drink" ~> "common:Root" % 1) + ("taaable:Food" ~> "common:Root" % 1)

    for {
      x <- instances.par
    } {
      val d1 = structuralCotopic(g, "taaable:Celery", shortenUri(x))
      val d2 = 1.0 - wuPalmer(g, "common:Root", "taaable:Celery", shortenUri(x))
      val (l, p1, p2) = lcs(g, "taaable:Celery", shortenUri(x)).get
      println(f"$x $d1 $d2 (via $l - $d1 - $d2)")
    }

    val res = for {
      x <- instances
      r = for ((l, p1, p2) <- lcsCandidates(g, "taaable:Celery", shortenUri(x)) take (3)) yield (l, weight(p1) + weight(p2))
    } yield (x -> r)

    res.toMap foreach println
    //  (http://dbpedia.org/resource/Celery,List((category:Stem_vegetables,2), (category:Vegetables,4), (category:Edible_plants,5)))
    //  (http://dbpedia.org/resource/Cel-Ray,List((category:Cuisine,7), (category:Food_and_drink,8), (category:Food_and_drink_preparation,14)))
    //  (http://dbpedia.org/resource/Celery_salt,List((category:Foods,6), (category:Food_and_drink,8), (category:Food_and_drink_preparation,10)))
    //  (http://dbpedia.org/resource/Celery_Victor,List((category:Foods,5), (category:Cuisine,7), (category:Food_and_drink,8)))
    //  (http://dbpedia.org/resource/Celery_cabbage,List((category:Vegetables,4), (category:Plants,8), (category:Botany,9)))
    //  (http://dbpedia.org/resource/Celeriac,List((category:Vegetables,4), (category:Edible_plants,5), (category:Crops,5)))
    //  (http://dbpedia.org/resource/Celebrity_(tomato),List((category:Vegetables,6), (category:Edible_plants,7), (category:Domesticated_plants,7)))

  }

}
