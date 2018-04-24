//import sys.process._
//import org.apache.spark.graphx._
//import java.io._

//import scala.io.Source
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.DataFrame
//import org.apache.spark._
//import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.feature.{StringIndexer,OneHotEncoder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}

object Preprocess {
  def main(args:Array[String]) : Unit = {

    if (args.length != 2) {
      println("dude, i need two parameters")
    }

    val input = args(0)
    val output = args(1)

    val spark = SparkSession
      .builder()
      .appName("Preprocess")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //    import spark.sqlContext.implicits._

    val filePath = input
    val df = spark.read.option("header","true").option("inferSchema","true").csv(filePath)

    df.printSchema

    /**
      * -----------------------------------------------------------------------------------
      * | ip | app | device | os | channel | click_time | attributed_time | is_attributed |
      * -----------------------------------------------------------------------------------
      */
    val df_lessfeatures = df//.withColumn("minute", minute(col("click_time")))
      .withColumn("hour",hour(col("click_time")))
      .withColumn("day",dayofmonth(col("click_time")))
      .drop("click_time","attributed_time")
      .withColumnRenamed("is_attributed", "label")

    /**
      * -----------------------------------------------------------------------------------
      * | ip | app | device | os | channel | hour | day | label (is_attributed) |
      * -----------------------------------------------------------------------------------
      */


    df_lessfeatures.show()

    val df1 = df_lessfeatures
    val df_morefeatures = df1
      // single features
      .join(df1.groupBy("ip").agg(count("*") as "ip_count"),"ip")
      .join(df1.groupBy("ip").agg(mean("label") as "ip_freq"),"ip")
      .join(df1.groupBy("app").agg(count("*") as "app_count"),"app")
      .join(df1.groupBy("app").agg(mean("label") as "app_freq"),"app")
      .join(df1.groupBy("device").agg(count("*") as "device_count"),"device")
      .join(df1.groupBy("device").agg(mean("label") as "device_freq"),"device")
      .join(df1.groupBy("os").agg(count("*") as "os_count"),"os")
      .join(df1.groupBy("os").agg(mean("label") as "os_freq"),"os")
      .join(df1.groupBy("channel").agg(count("*") as "channel_count"),"channel")
      .join(df1.groupBy("channel").agg(mean("label") as "channel_freq"),"channel")
      .join(df1.groupBy("hour").agg(count("*") as "hour_count"),"hour")
      .join(df1.groupBy("hour").agg(mean("label") as "hour_freq"),"hour")
      .join(df1.groupBy("day").agg(count("*") as "day_count"),"day")
      .join(df1.groupBy("day").agg(mean("label") as "day_freq"),"day")

      // Combined multiple features
      .join(df1.groupBy("ip", "hour", "device").agg(count("*") as "nip_h_dev"), Seq("ip", "hour", "device"))
      .join(df1.groupBy("ip", "hour", "app").agg(count("*") as "nip_h_app"), Seq("ip", "hour", "app"))
      .join(df1.groupBy("ip", "hour", "os").agg(count("*") as "nip_h_os"), Seq("ip", "hour", "os"))

      .join(df1.groupBy("ip", "os", "device").agg(count("*") as "User_count"), Seq("ip", "os", "device"))
      .join(df1.groupBy("ip", "os", "device").agg(mean("label") as "User_freq"), Seq("ip", "os", "device"))

      .join(df1.groupBy("ip", "day", "hour", "app").agg(count("*") as "nipApp"), Seq("ip", "day", "hour", "app"))
      .join(df1.groupBy("ip", "day", "hour", "app", "os").agg(count("*") as "nipAppOs"), Seq("ip", "day", "hour", "app", "os"))
      .join(df1.groupBy("app", "day", "hour").agg(count("*") as "napp"), Seq("app", "day", "hour"))
      .join(df1.groupBy("app", "day", "device").agg(count("*") as "nappDev"), Seq("app", "day", "device"))
      .join(df1.groupBy("app", "day", "device", "os").agg(count("*") as "nappDevOs"), Seq("app", "day", "device", "os"))

    df_morefeatures.show

    //------------------------------------------------------------------------

    val df_train = df_lessfeatures  // or df_lessfeatures to save your time

    //---------------------------------------------------------------------------

    //  ip|app|device| os|channel|label|hour|day|
    val categoricalColumns = Array("ip", "app", "device", "os", "channel", "hour","day")

    val numericCols = (df_train.columns.toSet -- categoricalColumns.toSet - "label").toArray
//    val numericCols = (df_lessfeatures.columns.toSet -- categoricalColumns.toSet - "label").toArray

    var stages = new ArrayBuffer[org.apache.spark.ml.PipelineStage]()

    for (categoricalCol <- categoricalColumns) {

      val stringIndexer = new StringIndexer()
        .setInputCol(categoricalCol)
        .setOutputCol(categoricalCol+"_Index")
        .setHandleInvalid("keep")  //   options are "keep", "error" or "skip"
      //   Category Indexing with StringIndexer
      val encoder = new OneHotEncoder()
        .setInputCol(categoricalCol+"_Index")
        .setOutputCol(categoricalCol+"_Vec")

      stages = stages ++ Array(stringIndexer,encoder)
    }

    val assemblerInputs = categoricalColumns.map( {x:String => x + "_Vec"})  ++ numericCols
    val assembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")
    stages += assembler

    val preprocess_pipeline = new Pipeline()
      .setStages(stages.toArray)

    val preprocess_model: PipelineModel = preprocess_pipeline.fit(df_train)

    // -- until this point we don't need to rebuild model again.

    // Get Feature Vector from Training Data
    val df_featuresvector = preprocess_model.transform(df_train)
    df_featuresvector.show

    val labelfeatures = df_featuresvector.select("label", "features")
    labelfeatures.show

    // Save the model

    // Now we can optionally save the fitted pipeline to disk
//    preprocess_model.write.overwrite().save(output +"_preprocess_model")

    val model_path = output + "_preprocess_model_alpha"
    preprocess_model.save(model_path)
    println(model_path)
    sc.parallelize(Seq(preprocess_model), 1).saveAsObjectFile(output +"_preprocess_model")

    // We can also save this unfit pipeline to disk
//    preprocess_pipeline.write.overwrite().save(output + "_preprocess_pipeline")

    sc.parallelize(Seq(preprocess_pipeline), 1).saveAsObjectFile(output +"_preprocess_pipeline")

    // And load it back in during production
    //val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")


    labelfeatures.write
      .option("header","true")
      .mode(SaveMode.Overwrite)
      .format("parquet")
      .save(output + "_labelfeatures.parquet")

    sc.stop()

  }
}




