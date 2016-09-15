package dsbook.sentimentanalysis

import com.google.common.collect.Iterables
import org.scalatest._
import org.scalatest.matchers.Matchers

class SentimentAnalyzerTest extends FlatSpec with Matchers {

  "Multiple sentences " should "yield multiple sentences" in {
    val doc = "I had a great time.  Overall, I think the movie was very good."
    val sentences = SentimentAnalyzer.splitIntoSentences(doc).toSeq
    assert(sentences.size == 2)
    assert(sentences(0).equals("I had a great time ."))
    assert(sentences(1).equals("Overall , I think the movie was very good ."))
  }

  "A positive document" should "be positive" in {
    val doc = "I had a great time.  Overall, I think the movie was very good."
    val sentiment = SentimentAnalyzer.analyzeDocument(doc)
    assert(sentiment.equals("POSITIVE"))
  }

  "A negative sentence " should "be negative" in {
    val doc = "I had a terrible time.  Overall, I think the movie was just terrible. " +
      "The popcorn was rancid.  " +
      "The person in front of me was loud and obnoxious."
    val sentiment = SentimentAnalyzer.analyzeDocument(doc)
    assert(sentiment.equals("NEGATIVE"))
  }
  "Test" should "be correct" in {
    val doc = "I love this movie!! Sure I love it because of Madonna but who cares - it's damn funny!!! *ALANiS Rocks*. When I first saw this film in the theatres back in 1987, I thought it was all out hilarious! Madonna is so funny and I love her dubbed accent and wacky/funky look. The all-time funniest part is when Madonna(Nikki) screams at a man who is about to get into a taxi. And also when Griffin Dunne(Louden)trips and falls at the apartment interview scene. **ALANiS Rocks**. Madonna's character Nikki steals/shop lifts and fools people throughout the whole movie - her hilarious antics are enough to keep you on the floor the whole time. \"Didn't rob nothin', when you rob a store you stick up the cashier. We busted a few tapes, there's a bit of a difference\" I love that!!! It's classic. ***ALANiS Rocks***. I don't know why this movie got slammed the way it did. I see nothing wrong with it - course maybe if you're a huge Madonna fan then whatever she does is just awesome. Anyone out there who wants to see some funny, classic entertainment then watch \"Who's That Girl?\" And another very important fact that of which should be known to all man kind or at least to all that exist, ALANiS will always \"rock ya\" completely to the end! So does Madonna in this film, and just entirely! Her acting is superb!";
    val sentiment = SentimentAnalyzer.analyzeDocument(doc)
    assert(sentiment.equals("POSITIVE"))
  }
}
