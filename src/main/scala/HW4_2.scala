import org.apache.spark.graphx._
import org.apache.spark.sql.{SaveMode, SparkSession}


object HW4_2 {
  def main(args:Array[String]) : Unit = {

    if (args.length != 2) {
      println("dude, i need two parameters")
    }
    val input = args(0)
    val output = args(1)
    val spark = SparkSession
      .builder()
      .appName("HW4_2")
                  .master("local")
      .getOrCreate()

    val sc =spark.sparkContext
    sc.setLogLevel("ERROR")
    //     val graph = GraphLoader.edgeListFile(sc, "file:/tmp/wiki-Vote.txt")
    val graph = GraphLoader.edgeListFile(sc, input)

    // a. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing edges in each
    val Q1 = graph.outDegrees.sortBy(-_._2).take(5)

    // b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
    // in each
    val Q2  = graph.inDegrees.sortBy(-_._2).take(5)

    // c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank
    // values. You are free to define the threshold parameter.
    val Q3 = graph.pageRank(0.001).vertices.sortBy(-_._2).take(5)

    // d. Run the connected components algorithm on it and find the top 5 components with the largest
    // number of nodes.
    val Q4 = graph.connectedComponents().vertices.sortBy(-_._2).take(5)

    // e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the
    // largest triangle count. In case of ties, you can randomly select the top 5 vertices.
    val Q5 = graph.triangleCount().vertices.sortBy(-_._2).take(5)

    // output to files ----------

    val str =
      "1. Outdegree\n" +
        Q1.mkString("\n") + "\n" +
        Q2.mkString("\n") + "\n" +
        "\n3. PageRank\n" +
        Q3.mkString("\n")+ "\n" +
        "\n4.Connected components\n"+
        Q4.mkString("\n")+ "\n" +
        "\n5.Triangle counts\n" +
        Q5.mkString("\n")+ "\n"

    sc.parallelize(Seq(str)).saveAsTextFile(output)

//    import spark.sqlContext.implicits._
//    Seq(str).toDF().write.option("header","false").mode(SaveMode.Overwrite).text(output)

    spark.stop()
  }
}