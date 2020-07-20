from datetime import datetime
# Start time of training the model 

start = datetime.now()

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml.feature import Word2Vec
from pyspark.sql.functions import split, regexp_replace
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace, split, udf
from pyspark.ml.feature import Word2Vec
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IDF
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.feature import PCA
from datetime import datetime
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import *
from pyspark.ml import Pipeline
import re
import copy

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Large Scale Project ") \
    .getOrCreate()


#Input files 
inputFileHire ='/home/mose/Downloads/CSVS/client_hiring_dt.csv' #"/user/user17/mosedata_proj/input/client_hiring_dt.csv"
inputFileBio ='/home/mose/Downloads/CSVS/client_bio_dt.csv'  #"/user/user17/mosedata_proj/input/client_bio_dt.csv"
inputFileCom = '/home/mose/Downloads/CSVS/client_communication_dt.csv' #"/user/user17/mosedata_proj/input/client_communication_dt.csv"
inputFileAct = '/home/mose/Downloads/CSVS/client_activities_dt.csv' #"/user/user17/mosedata_proj/input/client_activities_dt.csv"
inputFileFact = '/home/mose/Downloads/CSVS/client_fact_ft.csv' #"/user/user17/mosedata_proj/input/client_fact_ft.csv"


# Create DataFrame from CSV file
dfHire =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileHire)
dfBio =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileBio)
dfCom =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileCom)
dfAct =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileAct)
dfFact =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileFact)


# Print the schema of the DataFrames
dfHire.printSchema() 
dfBio.printSchema()
dfCom.printSchema()
dfAct.printSchema()
dfFact.printSchema()


#Temporary objects from which sql statements can be run for each dataframe
dfHire.createOrReplaceTempView("dfHire_sql")
dfBio.createOrReplaceTempView("dfBio_sql")
dfCom.createOrReplaceTempView("dfCom_sql")
dfAct.createOrReplaceTempView("dfAct_sql")
dfFact.createOrReplaceTempView("dfFAct_sql")

#Hired by Service branch
spark.sql(""" Select count(hires.id) as hired,bio.service_branch__c 
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 1
              group by bio.service_branch__c
              order by hired asc
              """).show()

#Not hired by Service Branch
spark.sql(""" Select count(hires.id) as Not_hired,bio.service_branch__c 
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 0
              group by bio.service_branch__c
              order by Not_hired asc
              """).show()

#Hired by rank
spark.sql(""" Select count(hires.id) as hired,bio.service_rank__c
              From dfHire_sql hires 
              inner join dfBio_sql bio on bio.id = hires.id
              Where hires.hired = 1
              group by bio.service_rank__c
              order by hired desc
              """).show()

#Prefered method of contact for the hired
spark.sql(""" select count(com.id) as count, com.preferred_method_of_contact__c 
              from dfCom_sql com 
              inner join dfHire_sql hires on com.id = hires.id
              where hires.hired = 1
              group by com.preferred_method_of_contact__c 
              order by count
          """).show()


#Prefered method of contact for the Unhired
spark.sql(""" select count(com.id) as count, com.preferred_method_of_contact__c 
              from dfCom_sql com 
              inner join dfHire_sql hires on com.id = hires.id
              where hires.hired = 0
              group by com.preferred_method_of_contact__c 
              order by count
          """).show()

#Select variables that determine whether one is going to be employed or not, store those varibles in a new dataframe
dfspark= spark.sql(""" select fact.yearsinservice as S_years , fact.reg_afterservice_years as R_years,
               act.resume_tailoring_tips__c as Resume_Tips,act.finalized_hhusa_revised_resume_on_file__c as Resume_OnFile, bio.client__c as Client, bio.client_type__c as Client_Type, bio.service_rank__c as Service_R
              ,bio.service_branch__c as Service_B, hire.hire_heroes_usa_confirmed_hire__c as HHUSA_hire, com.preferred_method_of_contact__c as Com_Method, com.responsive__c
              ,com.active__c, act.created_linkedin_account__c as Created_Linkedin, bio.highest_level_of_education_completed__c as Educ,  hire.hired
              ,bio.primary_military_occupational_specialty__c as Occupation
               
              from dfFact_sql fact
              
              inner join dfAct_sql act on act.id = fact.id
              inner join dfBio_sql bio on bio.id = fact.id
              inner join dfHire_sql hire on hire.id = fact.id
              inner join dfCom_sql com on com.id = fact.id
              
              where bio.client_type__c != '' and bio.service_branch__c !='' and bio.service_rank__c !='' 
              and com.preferred_method_of_contact__c !='' and fact.yearsinservice < 53 and fact.yearsinservice > 0 
              and fact.reg_afterservice_years >- 32 and fact.reg_afterservice_years < 53 and com.active__c !=''
              and bio.highest_level_of_education_completed__c !='' and hire.hired  !='' and hire.hired !='No' and hire.hired != 3

          """)
dfspark.persist()

dfsparkcopy = dfspark

# Get the number of clients in the description above
dfspark.count()

# Change the data type of the the hired column from string to double
dfspark = dfspark.withColumn("hired", dfspark["hired"].cast(DoubleType()))

# Confirm the labels in the hired column 0 represents the client was not hired 1 represents they were hired. 
dfspark.select('hired').distinct().show()


# The data types of the data frame 
print(" ")
print("The data types of the data frame. We need to convert them to indexes \
      vectors and then assemble into features, scale then apply to Machine learning algorithms. They accept only vectors" )
print(" ") 
print (dfspark.dtypes)
print(" ")
#Transform the data frame categorical columns to indexes using the StringIndexer method. 
indexers = [StringIndexer(inputCol=i, outputCol=i+"Index").fit(dfspark) \
            for i in list(set(dfspark.columns)- set(['S_years','R_years','Resume_Tips','Resume_OnFile','Client','HHUSA_hire','responsive__c','Created_Linkedin','Occupation','Educ'])) ]
pipeline = Pipeline(stages=indexers)
dfspark = pipeline.fit(dfspark).transform(dfspark)


#One hot encode the converted columns
encoder = OneHotEncoder(inputCols=["Service_BIndex", "Com_MethodIndex","active__cIndex", "Service_RIndex", "Client_TypeIndex"],
                       outputCols=["Service_BIndexVec", "Com_MethodIndexVec","active__cIndexVec", "Service_RIndexVec", "Client_TypeIndexVec"])
modelOne = encoder.fit(dfspark)
dfspark = modelOne.transform(dfspark)



#Convert the Education column from string to array of words using regular expressions. 
dfspark = dfspark.withColumn("Educ",split(regexp_replace("Educ", r"(^\[\[\[)|(\]\]\]$)", ""), ", ")
)

#Convert the words to vector in the Education column
word2Vec = Word2Vec(inputCol="Educ", outputCol="EducVec")
modelw = word2Vec.fit(dfspark)
dfspark = modelw.transform(dfspark)

# We can now put our newly created feature vectors  into one feature vector using the vectorizer. 
assembler2 = VectorAssembler(inputCols=['Resume_Tips','Resume_OnFile','Client','HHUSA_hire','responsive__c','Created_Linkedin','active__cIndexVec','Service_RIndexVec','Client_TypeIndexVec','Com_MethodIndexVec','Service_BIndexVec','EducVec'],outputCol="vecfeatures")
dfspark = assembler2.transform(dfspark)


# Scale the data uniformly
scaler2 = StandardScaler(inputCol="vecfeatures", outputCol="features")
scalerModel = scaler2.fit(dfspark)
dfspark = scalerModel.transform(dfspark)

#Train test split
train_data,test_data=dfspark.select('features','hired').randomSplit([0.7,0.3])

#Decision tree classifier will predict whether a client can be hired or not 1 or 0 using the hired and scaledFeatures colums
decision_tree = DecisionTreeClassifier(labelCol='hired', featuresCol='features')
model1 = decision_tree.fit(train_data)
pred = model1.transform(test_data)

#Check the new columns of the dataframe 
pred.columns


# Now lets evaluate the algorithm performance
def evaluate_algo(df,label,Algorithm_name  ):
    classification_evaluator = MulticlassClassificationEvaluator(
    labelCol=label, predictionCol="prediction")
    accuracy = classification_evaluator.evaluate(df)
    print(Algorithm_name )
    print("Test Error  = %g " % (1.0 - accuracy))
    print("Accuracy   = %g " % accuracy)

print(" ")
#Evaluate the algorithm performance 
evaluate_algo(pred,'hired','Decision Tree')

#Compare with another machine learning algorithm Random forests 
print(" ")
random_forest = RandomForestClassifier(labelCol='hired', featuresCol='features', numTrees=28)
model2= random_forest.fit(train_data)
predrand = model2.transform(test_data)

#Columns predicted for random forests
predrand.columns

#Evaluate the performance of random forests
evaluate_algo(predrand,'hired','Random Tree')


#Clustering 
funchash = FeatureHasher(inputCols= ['S_years','R_years','Resume_Tips','Resume_OnFile','Client','Client_Type','Service_R',\
 'Service_B','HHUSA_hire','Com_Method','responsive__c','active__c','Created_Linkedin','Educ','Occupation'],
                       outputCol="features")

dfsparkcopy = funchash.transform(dfsparkcopy)

def cluster(k,df,features):
    """ Takes in k as an int, the dataframe, and features """
    kmeans = KMeans(k=k, seed=1)  # 2 clusters here
    model = kmeans.fit(df.select(features))
    return model

transformeddf = cluster(4,dfsparkcopy,'features').transform(dfsparkcopy)

#Number of clusters
print(" ")
print ("Number of clusters from Kmeans")
transformeddf.select('prediction').distinct().show()

#Service branch with Service Rank, Showing whether hired or not. 
print(" ")
print(" Service branches with Service Ranks and cluster as prediction")
transformeddf.select('Service_B','Service_R','hired','prediction').distinct().show()


#Information about individual clusters in term of years service men stay in the army and when they register for services
print(" ")
print ("Statistical Information about years in service and Registration for services with HHUSA")
transformeddf.select('S_years','R_years').describe().show()

#Get cluster information Cluster 0 in terms of years service men spend on service.
print(" ")
print ("Years clustered with cluster 0 ")
cluster0 = transformeddf.filter(transformeddf.prediction==0)
cluster0.select('S_years','R_years').describe().show()

#Get cluster information Cluster 1
print(" ")
print ("Years clustered with cluster 1 ")
cluster1 = transformeddf.filter(transformeddf.prediction==1)
cluster1.select('S_years','R_years').describe().show()

#Get cluster information Cluster 2
print(" ")
print ("Years clustered with cluster 2")
cluster2 = transformeddf.filter(transformeddf.prediction==2)
cluster2.select('S_years','R_years').describe().show()


#Get cluster information Cluster 3
print(" ")
print ("Years clustered with cluster 3 ")
cluster3 = transformeddf.filter(transformeddf.prediction==3)
cluster3.select('S_years','R_years').describe().show()


end = datetime.now()

print('Duration of script: {}'.format(end - start))
