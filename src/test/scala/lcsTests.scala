import org.apache.jena.riot.{Lang, RDFDataMgr}

import scalax.collection.GraphPredef._
import scalax.collection.edge.Implicits._

import PrefixHelper._
import Alg._

class lcsTests {

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
    val dbpedia = GraphFactory.from(RDFDataMgr.loadModel("file:///D:/Workspaces/Dev/ldif-evaluation/ldif-taaable/test-celery.nt", Lang.NTRIPLES))

    val g = dbpedia ++ taaable ++
      merge("taaable:Food", "common:Food_and_drink") ++
      merge("taaable:Vegetable", "category:Vegetables") ++
      merge("taaable:Stalk_vegetable", "category:Stem_vegetables") ++
      merge("taaable:Leaf_vegetable", "category:Leaf_vegetables") +
      ("category:Food_and_drink" ~> "common:Root" % 1) + ("taaable:Food" ~> "common:Root" % 1)

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
