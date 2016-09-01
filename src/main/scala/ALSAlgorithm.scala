import io.prediction.controller.{P2LAlgorithm, Params}
import io.prediction.data.storage.BiMap
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

case class ALSAlgorithmParams(rank: Int, numIterations: Int, lambda: Double) extends Params

class Model(
             val model: MatrixFactorizationModel,
             val userStringIntMap: BiMap[String, Int],
             val itemStringIntMap: BiMap[String, Int]
           ) extends Serializable

class ALSAlgorithm(val params: ALSAlgorithmParams) extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  def train(sc: SparkContext, data: PreparedData): Model = {

    val userStringIntMap = BiMap.stringInt(data.favorites.map(_.userId))
    val itemStringIntMap = BiMap.stringInt(data.favorites.map(_.propertyId))

    val ratings = data.favorites.map { favorite =>
      Rating(userStringIntMap(favorite.userId), itemStringIntMap(favorite.propertyId), 1)
    }.cache()

    val model = ALS.train(ratings, params.rank, params.numIterations, params.lambda)

    new Model(model, userStringIntMap, itemStringIntMap)
  }

  def predict(model: Model, query: Query): PredictedResult = {
    val userIdInt = model.userStringIntMap(query.userId)

    val ratings = model.model.recommendProducts(userIdInt, query.numResults)

    val propertyRatings = ratings.map { rating =>
      model.itemStringIntMap.inverse(rating.product) -> rating.rating
    }

    PredictedResult(propertyRatings.toMap)
  }

}
