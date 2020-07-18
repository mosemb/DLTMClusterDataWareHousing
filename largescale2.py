
from datetime import datetime
# Start time of the script
start = datetime.now()

from pyspark.sql.functions import col, regexp_replace, split
from pyspark.sql import SparkSession
from pyspark.ml.feature import FeatureHasher
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import *
import copy




# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Large Scale Computing Application ") \
    .config('spark.yarn.executor.memoryOverhead', '2096')\
    .config('spark.driver.memory', '35g')\
    .config('spark.executor.cores', 4)\
    .getOrCreate()


# The dataset 
inputFileHire ="/user/user17/mosedata_proj/input/client_hiring_dt.csv"
inputFileBio ="/user/user17/mosedata_proj/input/client_bio_dt.csv"
inputFileCom = "/user/user17/mosedata_proj/input/client_communication_dt.csv"
inputFileAct = "/user/user17/mosedata_proj/input/client_activities_dt.csv"
inputFileFact = "/user/user17/mosedata_proj/input/client_fact_ft.csv"

# Create DataFrames from CSV files
dfHire =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileHire)
dfBio =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileBio)
dfCom =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileCom)
dfAct =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileAct)
dfFact =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileFact)

# Print the schemas of the DataFrames
dfHire.printSchema() 
dfBio.printSchema()
dfCom.printSchema()
dfAct.printSchema()
dfFact.printSchema()


#Clients from different service branches
dfBio.groupBy('service_branch__c').count().show()


#Types of jobs the guys got hired in
dfHire.groupBy('job_function_hired_in__c').count().show()


#Temporary objects from which sql statements can be run for each dataframe
dfHire.createOrReplaceTempView("dfHire_sql")
dfBio.createOrReplaceTempView("dfBio_sql")
dfCom.createOrReplaceTempView("dfCom_sql")
dfAct.createOrReplaceTempView("dfAct_sql")
dfFact.createOrReplaceTempView("dfFAct_sql")

#Account create date by month of the year
spark.sql( """SELECT count(id) , 
              month(create_ddate) as month 
              From  dfAct_sql  
              group by month 
              ORDER BY month ASC """).show()

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

# Get the number of clients in the description above
dfspark.count()

# Change the data type of the the hired column from string to double
dfspark = dfspark.withColumn("hired", dfspark["hired"].cast(DoubleType()))

# Confirm the labels in the hired column 0 represents the client was not hired 1 represents they were hired. 
dfspark.select('hired').distinct().show()


# The data types of the data frame 
dfspark.dtypes

#Hash the features through a hash function to convert the features into vectors for the dfspark dataframe,
# Convert strings to numerical data, the output column is features. The target is not included in this conversion.
funchash = FeatureHasher(inputCols= ['S_years','R_years','Resume_Tips','Resume_OnFile','Client','Client_Type','Service_R',\
 'Service_B','HHUSA_hire','Com_Method','responsive__c','active__c','Created_Linkedin','Educ','Occupation'],
                       outputCol="features")

#Add the new added features to the dataframe
dffeatures = funchash.transform(dfspark)


#Confirm that the dataframe is active, We are going to use this data to train our machine learning algorithms.
dffeatures.select('features','hired').show()

#Split the data into training and testing data for the training of the algorithms and testing of the algorithms 
train_data,test_data=dffeatures.randomSplit([0.75,0.25])


# Train the decision tree classifier, to predict whether one is hired or not, using the hired column and features. 
#def treedec(label, features):
  #  decision_tree = DecisionTreeClassifier(labelCol=label, featuresCol=features)
  #  return decision_tree

#decision_tree = DecisionTreeClassifier(labelCol='hired', featuresCol='features')
#model = decision_tree.fit(train_data)
#predictions = model.transform(test_data)

#def traindec_tree(train_data, algo): 
    # The model 
 #   model1 = algo.fit(train_data)
 #   return model1

# Make predictions using the test set of the dataset ... Store the predictions in a new dataframe prediction 
#predictions = traindec_tree(train_data, treedec('hired','features')).transform(test_data)

# Check out the new dataframe predictions to view the prediction, hired and features columns 
#predictions.select("prediction", "hired", "features").show()

# Now lets evaluate the algorithm performance
#def evaluate_algo(df,label,Algorithm_name  ):
 #   classification_evaluator = MulticlassClassificationEvaluator(
 #   labelCol=label, predictionCol="prediction")
 #   accuracy = classification_evaluator.evaluate(df)
#    print("Test Error  = %g " % (1.0 - accuracy))
  #  print("Accuracy   = %g " % accuracy)

#print(" ")
#evaluate_algo(predictions,'hired','Decision Tree')


#Random forest
def randomforest(label, features, numTrees):
    """ takes in labels, features and number of trees"""
	random_forest = RandomForestClassifier(labelCol=label, featuresCol=features, numTrees=numTrees)
	return random_forest

#Random forest fit function 
def random_forestfit(randf,train_data):
    """ Takes in a dataframe, and a random forest model """
	model= randf.fit(train_data)
	return model
 

# Make predictions.
predictions2 = random_forestfit(randomforest('hired', 'features', 10),train_data).transform(test_data)

# Select example rows to display.
#predictions2.select('prediction','hired','features').show()

#print(" ")
#evaluate_algo(predictions2,'hired','Random Forest')

#Clustering 
def cluster(k,df,features):
    """ Takes in k as an int, the dataframe, and features """
    kmeans = KMeans(k=k, seed=1)  # 2 clusters here
    model = kmeans.fit(df.select(features))
    return model

transformeddf = cluster(4,dffeatures,'features').transform(dffeatures)

#Number of clusters
print(" ")
print ("Number of clusters")
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

spark.stop()


