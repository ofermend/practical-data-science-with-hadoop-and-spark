import collection.JavaConversions._
import scala.collection.mutable

import opennlp.tools.tokenize.SimpleTokenizer
import opennlp.tools.stemmer.PorterStemmer

import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering.{OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, SparseVector, Vectors}
import org.apache.spark.mllib.feature.IDF

// add openNLP jar to the Spark Context 
sc.addJar("opennlp-tools-1.6.0.jar")

// Load documents from text files, 1 element (text string) per file
val corpus = sc.wholeTextFiles("ohsumed/C*", 20).map(x => x._2)

// read stop words from file
val stopwordFile = "stop-words.txt"
val st_words = sc.textFile(stopwordFile).collect()
      .flatMap(_.stripMargin.split("\\s+")).map(_.toLowerCase).toSet
val stopwords = sc.broadcast(st_words)

val minWordLength = 3
val tokenized: RDD[(Long, Array[String])] = corpus.zipWithIndex().map { case (text,id) => 
    val tokenizer = SimpleTokenizer.INSTANCE
    val stemmer = new PorterStemmer()    
    val tokens = tokenizer.tokenize(text)
    val words = tokens.filter(w => (w.length >= minWordLength) && (!stopwords.value.contains(w)))
                      .map(w => stemmer.stem(w))
    id -> words
}.filter(_._2.length > 0)

tokenized.cache()
val numDocs = tokenized.count()

val wordCounts: RDD[(String, Long)] = tokenized.flatMap { case (_, tokens) => 
tokens.map(_ -> 1L) 
}.reduceByKey(_ + _)
wordCounts.cache()
val fullVocabSize = wordCounts.count()
val vSize = 10000
val (vocab: Map[String, Int], selectedTokenCount: Long) = {
    val sortedWC: Array[(String,Long)] = {wordCounts.sortBy(_._2, ascending=false) .take(vSize)}
    (sortedWC.map(_._1).zipWithIndex.toMap, sortedWC.map(_._2).sum)
}


val documents = tokenized.map { case (id, tokens) =>
    // Filter tokens by vocabulary, and create word count vector representation of document.
    val wc = new mutable.HashMap[Int, Int]()
    tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
    }
    val indices = wc.keys.toArray.sorted
    val values = indices.map(i => wc(i).toDouble)
    val sb = Vectors.sparse(vocab.size, indices, values)
    (id, sb)
}

val vocabArray = new Array[String](vocab.size)
vocab.foreach { case (term, i) => vocabArray(i) = term }

val tf = documents.map { case (id, vec) => vec }.cache()
val idfVals = new IDF().fit(tf).idf.toArray
val tfidfDocs: RDD[(Long, Vector)] = documents.map { case (id, vec) =>
    val indices = vec.asInstanceOf[SparseVector].indices
    val counts = new mutable.HashMap[Int, Double]()    
    for (idx <- indices) {
        counts(idx) = vec(idx) * idfVals(idx)
    }
    (id, Vectors.sparse(vocab.size, counts.toSeq))
}



val numTopics = 5
val numIterations = 50
val lda = new LDA().setK(numTopics).setMaxIterations(numIterations).setOptimizer("online")
val ldaModel = lda.run(tfidfDocs)

val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 5)
topicIndices.foreach { case (terms, termWeights) =>
    println("TOPIC:")
    terms.zip(termWeights).foreach { case (term, weight) =>
        println(s"${vocabArray(term.toInt)}\t$weight")
    }
    println()
}




