import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

object Evaluation {
  def main(args:Array[String]) : Unit = {

    if (args.length != 1) {
      println("dude, i need three parameters")
    }

    val test_predict_data = args(0)

    val spark = SparkSession
      .builder()
      .appName("Evaluation")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val test_predict_df = spark.read
      .option("header", "true")
      .parquet(test_predict_data)

    test_predict_df.show(10)

    val evaluator = new BinaryClassificationEvaluator()
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val evaluatorParams = ParamMap(evaluator.metricName -> "areaUnderROC")
    val areaTest = evaluator.evaluate(test_predict_df, evaluatorParams)
    println("Evaluation: areaUnderROC " + areaTest.toString)
  }

}