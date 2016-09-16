from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier

# Initialize Spark
SparkContext.setSystemProperty('spark.executor.memory', '4g')
conf = SparkConf()
conf.set('spark.executor.instances', 20)
sc = SparkContext('yarn-client', 'kdd99', conf=conf)
hc = HiveContext(sc)

kdd = hc.table("kdd99")

(trainData, testData) = kdd.randomSplit([0.7, 0.3], seed=42)
trainData.cache()
services = trainData.withColumnRenamed('service','srvc') 				             .select('srvc').distinct()
testData = testData.join(services, testData.service==services.srvc)
            # filter out any rows with a service not trained upon
testData.cache()

print "training set has " + str(trainData.count()) + " instances"
print "test set has " + str(testData.count()) + " instances"

# Build model
inx1 = StringIndexer(inputCol="protocol", outputCol="protocol-cat")
inx2 = StringIndexer(inputCol="service", outputCol="service-cat")
inx3 = StringIndexer(inputCol="flag", outputCol="flag-cat")
inx4 = StringIndexer(inputCol="is_anomaly", outputCol="label")
ohe2 = OneHotEncoder(inputCol="service-cat", outputCol="service-ohe")
feat_cols = [c for c in kdd.columns + 
	      ['protocol-cat', 'service-ohe', 'flag-cat', 'label'] 
              if c not in ['protocol', 'service', 'flag', 'is_anomaly']]
vecAssembler = VectorAssembler(inputCols = feat_cols, 
				 outputCol = "features")

rf = RandomForestClassifier(numTrees=500, maxDepth=6, maxBins=80, seed=42)
pipeline = Pipeline(stages=[inx1, inx2, inx3, inx4, ohe2, vecAssembler, rf])
model = pipeline.fit(trainData)

# Evaluate model performance
def eval_metrics(lap):
    labels = lap.select("label").distinct().toPandas()['label'].tolist()
    tpos = [lap.filter(lap.label == x).filter(lap.prediction == x).count() for x in labels]
    fpos = [lap.filter(lap.label == x).filter(lap.prediction != x).count() for x in labels]
    fneg = [lap.filter(lap.label != x).filter(lap.prediction == x).count() for x in labels]
    precision = zip(labels, [float(tp)/(tp+fp+1e-50) for (tp,fp) in zip(tpos,fpos)])
    recall = zip(labels, [float(tp)/(tp+fn+1e-50) for (tp,fn) in zip(tpos,fneg)])
    return (precision,recall)

results = model.transform(testData).select("label", "prediction").cache()
(precision, recall) = eval_metrics(results)
ordered_labels = model.stages[3]._call_java("labels")
df = pd.DataFrame([(x, testData.filter(testData.is_anomaly == x).count(), y[1], z[1]) 
                   for x,y,z in zip(ordered_labels, sorted(precision, key=lambda x: x[0]), 
                                                    sorted(recall, key=lambda x: x[0]))],
                  columns = ['type', 'count', 'precision', 'recall'])
print df
