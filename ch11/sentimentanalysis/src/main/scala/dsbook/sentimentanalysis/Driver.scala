package dsbook.sentimentanalysis

import com.google.protobuf.Descriptors.EnumValueDescriptor
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.InputFormatInfo


/**
  * Using spark, provide the capability to extract sentiment for a corpus of documents and evaluate how well
  * the estimates are.
  */
object Driver {

  /**
    * Calculate the sentiments per-document for a corpus of documents
    * @param inputPath A corpus of documents
    * @return A RDD containing the original document and the associated sentiment
    */
  def getSentiment( inputPath:RDD[String]): RDD[Tuple2[String, String]] = {
    inputPath.flatMap( doc => SentimentAnalyzer.splitIntoSentences(doc).map( sentence => (doc, sentence)))
             .map( doc2sentence => (doc2sentence._1, SentimentAnalyzer.analyzeSentence(doc2sentence._2)))
             .groupBy( x => x._1)
             .map( doc2sentences => (doc2sentences._1, SentimentAnalyzer.rollup(doc2sentences._2.map(x => x._2))))
  }

  /**
    * Evaluate the sentiment prediction calculated
    * @param positiveDocs A corpus of positive documents
    * @param negativeDocs A corpus of negative documents
    * @return A Map containing the confusion matrix: # of x evaluated as y for x, y in (positive, negative)
    *         and the accuracy.
    */
  def evaluateSentiment(positiveDocs:RDD[String], negativeDocs:RDD[String]) : Map[String, Double]  = {
    val positiveClassifiedAsNegative= getError(positiveDocs, "POSITIVE")
    val positiveClassifiedAsPositive = 1.0 - positiveClassifiedAsNegative._1
    val negativeClassifiedAsPositive = getError(negativeDocs, "NEGATIVE")
    val negativeClassifiedAsNegative = 1.0 - negativeClassifiedAsPositive._1
    val n = positiveClassifiedAsNegative._2 + negativeClassifiedAsPositive._2
    val numErrors = Math.round( positiveClassifiedAsNegative._1*positiveClassifiedAsNegative._2
                              + negativeClassifiedAsPositive._1*negativeClassifiedAsPositive._2
                              )

    Map("POSITIVE CLASSIFIED AS NEGATIVE" -> 100.0*positiveClassifiedAsNegative._1
       ,"POSITIVE CLASSIFIED AS POSITIVE" -> 100.0*positiveClassifiedAsPositive
       ,"NEGATIVE CLASSIFIED AS POSITIVE" -> 100.0*negativeClassifiedAsPositive._1
       ,"NEGATIVE CLASSIFIED AS NEGATIVE" -> 100.0*negativeClassifiedAsNegative
       ,"ACCURACY" -> (100.0 - (100.0*numErrors)/n)
       )
  }


  def printAccuracy(results : Map[String, Double]
                   ) = {
    System.out.println("NEGATIVE CLASSIFIED AS NEGATIVE: " + results.get("NEGATIVE CLASSIFIED AS NEGATIVE").get)
    System.out.println("NEGATIVE CLASSIFIED AS POSITIVE: " + results.get("NEGATIVE CLASSIFIED AS POSITIVE").get)
    System.out.println("POSITIVE CLASSIFIED AS NEGATIVE: " + results.get("POSITIVE CLASSIFIED AS NEGATIVE").get)
    System.out.println("POSITIVE CLASSIFIED AS POSITIVE: " + results.get("POSITIVE CLASSIFIED AS POSITIVE").get)
    System.out.println("Accuracy: " + results.get("ACCURACY").get)
  }

  /**
    * Retrieve the % error and the number of elements
    * @param docs A corpus of documents
    * @param expected What we expect the documents to be
    * @return % error and the number of elements in the corpus.
    */
  def getError(docs:RDD[String], expected:String) : Tuple2[Double, Integer] = {
    val sentiments = getSentiment(docs)
    val numDocs = sentiments.count()
    val numCorrect = sentiments.filter( sentiment => !sentiment._2.equals(expected)).count()
    ( (1.0*numCorrect)/numDocs, numDocs.toInt )
  }


  /**
    * The main entry point of the driver. The arguments are as follows:
    * 1. mode : "evaluate" or "run".
    *
    * For the "evaluate" mode, the next two arguments are:
    * 1. Location in HDFS of the positive corpus of documents (one document per line of a text file)
    * 2. Location in HDFS of the negative corpus of documents (one document per line of a text file)
    *
    * Summary statistics of error are output by evaluating each of these corpuses and
    * computing the accuracy
    *
    * For the "run" mode, the next two arguments are:
    * 1. Location in HDFS of the input corpus (one document per line of a text file)
    * 2. Location in HDFS for the output corpus
    *
    * The corpus will be evaluated, document by document, and the results will be written out
    * in the form of a tab separated file with the first column being the initial document and
    * the second column being the sentiment, POSITIVE or NEGATIVE.
    * @param args
    */
  def main(args:Array[String]) = {
    val sparkConfig = new SparkConf()
      .setAppName("sentimentAnalysis")
      .setMaster("yarn-client")
    val sc = new SparkContext(sparkConfig)
    val mode = args(0)
    if( mode.equals("evaluate")) {
      val positiveDocs = sc.textFile(args(1))
      val negativeDocs = sc.textFile(args(2))
      printAccuracy(evaluateSentiment(positiveDocs, negativeDocs))
    }
    else {
      val input = sc.textFile(args(1))
      val output = args(2)
      getSentiment(input).map(x => x._1 + "\t" + x._2)
                         .saveAsTextFile(output)
    }

  }
}
