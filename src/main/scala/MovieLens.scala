    /************************************************************
      * This class requires two arguments:
      *  input file - can be downloaded from http://www.utdallas.edu/~axn112530/cs6350/data/movielens/ratings.dat
      *  output location - can be on S3 or cluster
    *************************************************************/

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object MovieLens {

  def main(args: Array[String]) {


    def parseRating(str: String): Rating = {
      val fields = str.split("::")
      assert(fields.size == 4)
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    if (args.length == 0) {
      println("i need two two parameters ")
    }

    val spark = SparkSession
      .builder()
      .appName("MovieLens")
      .getOrCreate()

    import spark.implicits._

    val ratings = spark.read.textFile(args(0)).map(parseRating).toDF()


    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    // Generate top 10 movie recommendations for each user
    val userRecs = model.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = model.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    userRecs.rdd.repartition(1).saveAsTextFile(args(1))
  }

}
