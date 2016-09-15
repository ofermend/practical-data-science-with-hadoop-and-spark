package dsbook.sentimentanalysis

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import org.apache.spark.SparkContext

class DriverTest extends FunSuite with LocalSparkContext {

  test("test completely accurate") {
    val positiveDocs = List("I had a great time!"
                           ,"I loved the movie.  I thought it was the best I've ever seen!"
                           , "My favorite part of the movie was the first part.  It was the best."
                           )
    val negativeDocs = List("I had a terrible time!"
                           ,"I hated the movie.  I thought it was the worst I've ever seen!"
                           , "My least favorite part of the movie was the first part.  It was the worst."
                           )
    val positiveRDD = sc.makeRDD(positiveDocs)
    val negativeRDD = sc.makeRDD(negativeDocs)
    val results = Driver.evaluateSentiment(positiveRDD, negativeRDD)
    val accuracy = results.get("ACCURACY").get
    //6 / 6 correct
    assert(Math.abs(accuracy - 100*(6.0/6)) < 1e-6)
  }
  test("test one miss in each category") {
    val positiveDocs = List("I had a great time!"
      ,"I hated the movie.  I thought it was the worst I've ever seen!"
                           , "My favorite part of the movie was the first part.  It was the best."
                           )
    val negativeDocs = List("I had a terrible time!"
      ,"I loved the movie.  I thought it was the best I've ever seen!"
                           , "My least favorite part of the movie was the first part.  It was the worst."
                           )
    val positiveRDD = sc.makeRDD(positiveDocs)
    val negativeRDD = sc.makeRDD(negativeDocs)
    val results = Driver.evaluateSentiment(positiveRDD, negativeRDD)
    val accuracy = results.get("ACCURACY").get
    //4 correct, 2 wrong
    assert(Math.abs(accuracy - 100*(4.0/6)) < 1e-6)
  }
}
