from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrame basic example") \
    .getOrCreate()
 
inputFile = "hdfs://master:9000/user/hadoop/movie_dataset/movie.csv"
 
# Create DataFrame from CSV file
dfCsv =  spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load(inputFile)

# Print the schema of the DataFrame
dfCsv.printSchema()

print dfCsv.count()

# Stop the Spark Session
spark.stop()
