from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/home/user17/postgresql-42.2.14.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://192.168.98.45:5432/datawarehouse_hireheroes") \
    .option("dbtable", "client_hiring_dt") \
    .option("user", "postgres") \
    .option("password", "123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()
