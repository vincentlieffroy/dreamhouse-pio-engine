import io.prediction.controller.{Engine, EngineFactory}

case class Query(userId: String, numResults: Int)

case class PredictedResult(propertyRatings: Map[String, Double])

case class ItemScore(
  item: String,
  score: Double
) extends Serializable

object SimilarProductEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("als" -> classOf[ALSAlgorithm]),
      classOf[Serving]
    )
  }
}
