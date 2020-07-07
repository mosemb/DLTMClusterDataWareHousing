from pyspark.sql import SparkSession
from pyspark.sql import Window

# Initialize SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrame from Postgres Table") \
    .getOrCreate()

# Load the bookings table into a DaraFrame
jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://master:5432/clubdata") \
    .option("dbtable", "cd.bookings") \
    .option("user", "student") \
    .option("password", "student") \
    .load()

# Print the schema of the DataFrame
jdbcDF.printSchema()

# Print the number of rows
print jdbcDF.count()

# Stop the Spark Session
spark.stop()
