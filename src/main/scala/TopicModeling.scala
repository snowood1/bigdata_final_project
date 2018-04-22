/************************************************************
  * This class requires two arguments:
  *  input file - can be downloaded from http://www.utdallas.edu/~axn112530/cs6350/data/topicModeling/sparkDocs
  *  output location - can be on S3 or cluster
  *************************************************************/


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD



import scala.collection.mutable

object TopicModeling {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("topicModel"))

    if (args.length == 0) {
      println("i need two parameters ")
    }


    val corpus: RDD[String] = sc.wholeTextFiles(args(0)).map(_._2)

    // Split each document into a sequence of terms (words)
    val stopWordSet = StopWordsRemover.loadDefaultStopWords("english").toSet
    val tokenized: RDD[Seq[String]] =
      corpus.map(_.toLowerCase.split("\\s")).map(_.filter(_.length > 3).filter(token => !stopWordSet.contains(token)).filter(_.forall(java.lang.Character.isLetter)))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 10
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    var output = ""
    topicIndices.foreach { case (terms, termWeights) =>
      output += "TOPIC:"
      terms.zip(termWeights).foreach { case (term, weight) =>
        output += {vocabArray(term.toInt)}
        output += "\t" + weight + "\n"
      }
      output += "\n\n"
    }

    sc.parallelize(List(output)).saveAsTextFile(args(1))

  }

}
