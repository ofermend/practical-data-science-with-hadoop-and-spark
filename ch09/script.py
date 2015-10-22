
import re, string
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StringType, ArrayType, FloatType
import pyspark.sql.functions as F
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler, IDF, RegexTokenizer, HashingTF
from pyspark.ml import Pipeline

# Set up SparkContext
SparkContext.setSystemProperty('spark.executor.memory', '8g')
conf = SparkConf()
conf.set('spark.executor.instances', 8)
conf.set('spark.sql.autoBroadcastJoinThreshold', 400*1024*1024)  # 400MB for broadcast join
sc = SparkContext('yarn-client', 'ch9-demo', conf=conf)

# Setup HiveContext
from pyspark.sql import HiveContext
hc = HiveContext(sc)
hc.sql("use demo")

# Define PySpark UDF to tokenize text into words with various other specialized procesing
punct = re.compile('[%s]' % re.escape(string.punctuation))
def tok_str(text, ngrams=1, minChars=2):
    text = re.sub(r'\s+', ' ', text) 		     # change any whitespace to regular space
    tokens = map(unicode, text.lower().split(' '))     # split into tokens and change to lower case
    tokens = filter(lambda x: len(x)>=minChars and x[0]!='@', tokens)     
                                                       # remove short words and usernames
    tokens = ["URL" if t[:4]=="http" else t for t in tokens]      
     # repalce any url by the constant word "URL"
    tokens = [punct.sub('', t) for t in tokens]        # remove punctuation from tokens
    if ngrams==1:
        return tokens
    else:
        return tokens + [' '.join(tokens[i:i+ngrams]) for i in xrange(len(tokens)-ngrams+1)]
tokenize = F.udf(lambda s: tok_str(unicode(s),ngrams=2), ArrayType(StringType()))

# Load sentiment dictionary
wv = hc.table('sentiment_words').collect()
wordlist = dict([(r.word,r.score) for r in wv])

# get positive sentiment scores from words RDD using word-list
def pscore(words):
    scores = filter(lambda x: x>0, [wordlist[t] for t in words if t in wordlist])
    return 0.0 if len(scores)==0 else (float(sum(scores))/len(scores))
pos_score = F.udf(lambda w: pscore(w), FloatType())

# get negative sentiment scores from words RDD using word-list
def nscore(words):
    scores = filter(lambda x: x<0, [wordlist[t] for t in words if t in wordlist])
    return 0.0 if len(scores)==0 else (float(sum(scores))/len(scores))
neg_score = F.udf(lambda w: nscore(w), FloatType()) 

# Create feature matrix for the model
tw1 = hc.sql("""
SELECT text, query, polarity, date,
       regexp_extract(date, '([0-9]{2}):([0-9]{2}):([0-9]{2})', 1) as hour,
       regexp_extract(date, '(Sun|Mon|Tue|Wed|Thu|Fri|Sat)', 1) as dayofweek,
       regexp_extract(date, '(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)', 1) as month
FROM tweets 
""")
tw2 = tw1.filter("polarity != 2").withColumn('words', tokenize(tw1['text']))
tw3 = (tw2.select("user", "hour", "dayofweek", "month", "words",
            	  F.when(tw2.polarity == 4, "Pos").otherwise("Neg").alias("sentiment"),
            	  pos_score(tw2["words"]).alias("pscore"), 	
	    	  neg_score(tw2["words"]).alias("nscore")))
tw3.registerTempTable("fm")

# paramaters for modeling
numFeatures = 5000
minDocFreq = 50
numTrees = 1000

# Build Machine Learning pipeline
inx1 = StringIndexer(inputCol="hour", outputCol="hour-inx")
inx2 = StringIndexer(inputCol="month", outputCol="month-inx")
inx3 = StringIndexer(inputCol="dayofweek", outputCol="dow-inx")
inx4 = StringIndexer(inputCol="sentiment", outputCol="label")
hashingTF = HashingTF(numFeatures=numFeatures, inputCol="words", outputCol="hash-tf")
idf = IDF(minDocFreq=minDocFreq, inputCol="hash-tf", outputCol="hash-tfidf")
va = VectorAssembler(inputCols =["hour-inx", "month-inx", "dow-inx", "hash-tfidf", "pscore", "nscore"], outputCol="features")
rf = RandomForestClassifier(numTrees=numTrees, maxDepth=4, maxBins=32, labelCol="label", seed=42)
p = Pipeline(stages=[inx1, inx2, inx3, inx4, hashingTF, idf, va, rf])

# Split feature matrix into train/test sets
(trainSet, testSet) = hc.table("fm").randomSplit([0.7, 0.3])
trainData = trainSet.cache()
testData = testSet.cache()
model = p.fit(trainData)                 # Train the model on training data

# Helper function to evaluate precision/recall/accuracy
def eval_metrics(lap):
    tp = float(len(lap[(lap['label']==1) & (lap['prediction']==1)]))
    tn = float(len(lap[(lap['label']==0) & (lap['prediction']==0)]))
    fp = float(len(lap[(lap['label']==0) & (lap['prediction']==1)]))
    fn = float(len(lap[(lap['label']==1) & (lap['prediction']==0)]))
    precision = tp / (tp+fp)
    recall = tp / (tp+fn)
    accuracy = (tp+tn) / (tp+tn+fp+fn)
    return {'precision': precision, 'recall': recall, 'accuracy': accuracy}

results = model.transform(testData)      # Predict using test data
lap = results.select("label", "prediction").toPandas()

m = eval_metrics(lap)
print m


