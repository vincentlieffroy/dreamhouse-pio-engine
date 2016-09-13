import io.prediction.controller.PPreparator
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class PreparedData(favorites: RDD[Favorite])

class Preparator extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    PreparedData(trainingData.favorites)
  }

}
