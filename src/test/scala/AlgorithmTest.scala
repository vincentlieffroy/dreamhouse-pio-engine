import org.scalatest.FlatSpec
import org.scalatest.Matchers

class AlgorithmTest extends FlatSpec with EngineTestSparkContext with Matchers {

  val params = ALSAlgorithmParams(10, 10, 0.01)
  val algorithm = new ALSAlgorithm(params)

  val favorites = Seq(
    Favorite("p1", "c1"),
    Favorite("p1", "c2"),
    Favorite("p2", "c2"),
    Favorite("p1", "c3"),
    Favorite("p2", "c3"),
    Favorite("p3", "c3")
  )

  "train" should "work" in {

    val rdd = sc.parallelize(favorites)

    val preparedData = PreparedData(rdd)

    val model = algorithm.train(sc, preparedData)

    model.userFeatures.length should be (3)
    model.productFeatures.length should be (3)
  }

  "predict" should "work" in {
    val rdd = sc.parallelize(favorites)

    val preparedData = PreparedData(rdd)

    val model = algorithm.train(sc, preparedData)

    println(model)

    val query = Query("c1", 10)

    val predictedResult = algorithm.predict(model, query)

    println(predictedResult)

    predictedResult.propertyRatings.size should be (3)

    val sortedProperties = predictedResult.propertyRatings.toSeq.sortBy(_._2).map(_._1).reverse

    sortedProperties(0) should be ("p1")
    sortedProperties(1) should be ("p2")
    sortedProperties(2) should be ("p3")
  }

}
