from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrame basic example") \
    .getOrCreate()
 
inputFileM = "hdfs://master:9000/user/hadoop/movie_dataset/movie.csv"
inputFileR = "hdfs://master:9000/user/hadoop/movie_dataset/rating.csv"
 
# Create DataFrame from CSV file
dfM =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileM)
dfR =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFileR)

# Print the schema of the DataFrames
dfM.printSchema()
dfR.printSchema()

dfR.groupBy('movieId').count().show(50)
dfM.where("director='Martin Brest'").join(dfR,"movieId").select("movieId","director","user_name","rating").show(50)


# Stop the Spark Session
spark.stop()
