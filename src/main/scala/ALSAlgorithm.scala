import io.prediction.controller.{P2LAlgorithm, Params, PersistentModel, PersistentModelLoader}
import io.prediction.data.storage.jdbc.JDBCModels
import io.prediction.data.storage.{BiMap, Model, Storage}
import io.prediction.workflow.KryoInstantiator
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

case class ALSAlgorithmParams(rank: Int, numIterations: Int, lambda: Double) extends Params

case class ALSModel(
             rank: Int,
             userFeatures: Array[(Int, Array[Double])],
             productFeatures: Array[(Int, Array[Double])],
             userStringIntMap: BiMap[String, Int],
             itemStringIntMap: BiMap[String, Int],
             @transient maybeMatrixFactorizationModel: Option[MatrixFactorizationModel]
           ) extends PersistentModel[ALSAlgorithmParams] {
  override def save(id: String, params: ALSAlgorithmParams, sc: SparkContext): Boolean = {
    val kryo = KryoInstantiator.newKryoInjection
    val bytes = kryo(this)
    ALSModel.jdbcModels.insert(Model(id, bytes))
    true
  }
}

object ALSModel extends PersistentModelLoader[ALSAlgorithmParams, ALSModel] {

  def jdbcModels = {
    val config = Storage.getConfig("PGSQL").get
    new JDBCModels(config.properties("URL"), config, "pio_model")
  }

  override def apply(id: String, params: ALSAlgorithmParams, maybeSparkContext: Option[SparkContext]): ALSModel = {
    val model = jdbcModels.get(id).get

    val kryo = KryoInstantiator.newKryoInjection

    val alsModel = kryo.invert(model.models).get.asInstanceOf[ALSModel]

    val userFeaturesRDD = maybeSparkContext.get.parallelize(alsModel.userFeatures)
    val productFeaturesRDD = maybeSparkContext.get.parallelize(alsModel.productFeatures)

    val matrixFactorizationModel = new MatrixFactorizationModel(alsModel.rank, userFeaturesRDD, productFeaturesRDD)

    alsModel.copy(maybeMatrixFactorizationModel = Some(matrixFactorizationModel))
  }
}

class ALSAlgorithm(val params: ALSAlgorithmParams) extends P2LAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  def train(sc: SparkContext, data: PreparedData): ALSModel = {

    val userStringIntMap = BiMap.stringInt(data.favorites.map(_.userId))
    val itemStringIntMap = BiMap.stringInt(data.favorites.map(_.propertyId))

    val ratings = data.favorites.map { favorite =>
      Rating(userStringIntMap(favorite.userId), itemStringIntMap(favorite.propertyId), 1)
    }.cache()

    val model = ALS.train(ratings, params.rank, params.numIterations, params.lambda)

    new ALSModel(model.rank, model.userFeatures.collect(), model.productFeatures.collect(), userStringIntMap, itemStringIntMap, Some(model))
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {

    val userIdInt = model.userStringIntMap(query.userId)

    val ratings = model.maybeMatrixFactorizationModel.get.recommendProducts(userIdInt, query.numResults)

    val propertyRatings = ratings.map { rating =>
      model.itemStringIntMap.inverse(rating.product) -> rating.rating
    }

    PredictedResult(propertyRatings.toMap)
  }

}
