from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from time import time

t0=time()
# conf = SparkConf().setAppName("MyApp")
#sc =  SparkContext(conf)
sc = SparkContext()
spark = SparkSession(sc)
# Prepare training documents, which are labeled.
train = spark.createDataFrame([
    (0, "compute with spark", 1.0),
    (1, "spark eggs", 0.0),
    (2, "spark is parallel", 1.0),
    (3, "hadoop mapreduce", 0.0),
    (4, "who is spark who", 0.0),
    (5, "handle spark data", 1.0),
    (6, "spark flies", 0.0),
    (7, "it was mapreduce", 0.0),
    (8, "the spark computer", 1.0),
    (9, "who", 0.0),
    (10, "spark compiler", 1.0),
    (11, "hadoop software", 0.0)
], ["id", "text", "label"])

tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

    
# Run cross-validation, and choose the best set of parameters.
# cvModel = crossval.fit(train)

# Prepare test documents, which are unlabeled.
test = spark.createDataFrame([
    (4, "spark computer"),
    (5, "spark animal"),
    (6, "program based on spark"),
    (7, "spark and hadoop are parallel")
], ["id", "text"])

# Make predictions on test documents. cvModel uses the best model found (lrModel).
#prediction = cvModel.transform(test)

prediction = pipeline.fit(train).transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
print("\n--------\n")
for row in selected.collect():
    print(row[1]," : ",row[3])

    
t1=time()
print("\n\n Execution Time: ",t1-t0)
