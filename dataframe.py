from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("DatawarehouseSpark Application") \
    .getOrCreate()
 
inputFileHire = "/user/user17/mosedata_proj/input/client_hiring_dt.csv"
inputFileBio = "/user/user17/mosedata_proj/input/client_bio_dt.csv"
inputFileCom = "/user/user17/mosedata_proj/input/client_communication_dt.csv"
inputFileAct = "/user/user17/mosedata_proj/input/client_activities_dt.csv"
inputFileFact = "/user/user17/mosedata_proj/input/client_fact_ft.csv"

 
# Create DataFrame from CSV file
dfM =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileHire)
dfR =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileBio)
dfCom =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileCom)
dfAct =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileAct)
dfFact =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileFact)


# Print the schema of the DataFrames
dfM.printSchema()
dfR.printSchema()
dfCom.printSchema()
dfAct.printSchema()
dfFact.printSchema()


#dfR.groupBy('movieId').count().show(50)
#dfM.where("director='Martin Brest'").join(dfR,"movieId").select("movieId","director","user_name","rating").show(50)


# Stop the Spark Session
spark.stop()
