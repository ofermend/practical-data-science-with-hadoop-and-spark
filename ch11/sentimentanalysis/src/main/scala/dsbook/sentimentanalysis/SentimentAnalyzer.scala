package dsbook.sentimentanalysis

import java.io.{OutputStream, PrintStream, StringReader}
import java.util
import java.util.Properties

import edu.stanford.nlp.ling.{CoreAnnotations, Sentence}
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.process.DocumentPreprocessor
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
import org.ejml.simple.SimpleMatrix

import scala.collection.JavaConversions._
import scala.collection.mutable


object SentimentAnalyzer {


  /**
    * Set up a sentiment pipeline using CoreNLP to tokenize, apply part-of-speech tagging and generate sentiment
    * estimates.
    */
  object SentimentPipeline {
    val props = new Properties
    props.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment")
    val pipeline = new StanfordCoreNLP(props)
  }

  /**
    * Split a document into sentences using CoreNLP
    * @param document
    * @return the sentences within the document
    */
  def splitIntoSentences(document:String) : Iterable[String] = {

    val err = System.err;

    // now make all writes to the System.err stream silent
    System.setErr(new PrintStream(new OutputStream() {
      def write(b : Int ) {
      }
    }));
    val reader = new StringReader(document);
    val dp = new DocumentPreprocessor(reader);
    val sentences = dp.map(sentence => Sentence.listToString(sentence))
    System.setErr(err);
    sentences
  }


  /**
    * Analyze a whole document by splitting the document into sentences, extracting
    * the sentiment and aggregating the sentiments into a total positive or negative score
    *
    * @param document
    * @return POSITIVE or NEGATIVE
    */
  def analyzeDocument(document: String) : String = {

    // this is your print stream, store the reference
    val err = System.err;

    // now make all writes to the System.err stream silent
    System.setErr(new PrintStream(new OutputStream() {
      def write(b : Int ) {
      }
    }));
    val pipeline = SentimentPipeline.pipeline
    val annotation = pipeline.process(document)
    System.setErr(err);

    rollup(
      annotation.get((new CoreAnnotations.SentencesAnnotation).getClass)
                .map(sentence => analyzeSentence(sentence))
    )
  }


  /**
    * Analyze an individual sentence using CoreNLP by extracting
    * the sentiment
    *
    * @param sentence
    * @return POSITIVE or NEGATIVE
    */
  def analyzeSentence(sentence: String) : String = {

    val err = System.err;

    System.setErr(new PrintStream(new OutputStream() {
      def write(b : Int ) {
      }
    }));

    val pipeline = SentimentPipeline.pipeline
    val annotation = pipeline.process(sentence)
    val sentiment = analyzeSentence(annotation.get((new CoreAnnotations.SentencesAnnotation).getClass).get(0))
    System.setErr(err);
    sentiment
  }

  /**
    * Analyze an individual sentence using CoreNLP by extracting
    * the sentiment
    *
    * @param sentence the probabilities for the sentiments
    * @return POSITIVE or NEGATIVE
    */
  def analyzeSentence(sentence: CoreMap) : String = {
    //for each sentence, we get the sentiment that CoreNLP thinks this sentence indicates.
    val sentimentTree = sentence.get((new SentimentCoreAnnotations.AnnotatedTree).getClass)
    val mat = RNNCoreAnnotations.getPredictions(sentimentTree)
    /*
    The probabilities are very negative, negative, neutral, positive or very positive.  We want the probability that
    the sentence is positive, so we choose to collapse categories as neutral, positive and very positive.
     */
    if(mat.get(2) > .5) {
      return "NEUTRAL"
    }
    else if(mat.get(2) + mat.get(3) + mat.get(4) >   .5) {
      return "POSITIVE"
    }
    else {
      return "NEGATIVE"
    }
  }

  /**
    * Aggregate the sentiments of a collection of sentences into a total sentiment
    * of a document.  Assume a rough estimate using majority rules.
    * @param sentencePositivityProbabilities
    * @return POSITIVE or NEGATIVE
    */
  def rollup(sentencePositivityProbabilities : Iterable[String]) : String = {
    var n = 0
    var numPositive = 0
    for( sentiment <- sentencePositivityProbabilities) {
      if(sentiment.equals("POSITIVE")) {
        numPositive = numPositive + 1
      }
      if(!sentiment.equals("NEUTRAL")) {
        n = n + 1
      }
    }
    if(numPositive == 0) {
      "NEUTRAL"
    }
    val score = (1.0*numPositive) / n
    if(score > .5) {
      "POSITIVE"
    }
    else {
      "NEGATIVE"
    }
  }
}
