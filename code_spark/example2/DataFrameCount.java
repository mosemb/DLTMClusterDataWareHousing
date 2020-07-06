package example2;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
 
public class DataFrameCount {
    public static void main(String[] args) throws Exception {
	String inputFile = "hdfs://master:9000/user/hadoop/movie_dataset/movie.csv";
 
	// Initialize Spark Session
	SparkSession spark = SparkSession
 	 .builder()
  	 .appName("Java Spark example with DataFrame")
  	 .getOrCreate();
 
	// Create a DataFrame reading the data from csv
	Dataset<Row> dfCsv = spark.read().format("csv")
  	 .option("sep", ",")
  	 .option("inferSchema", "true")
  	 .option("header", "true")
  	 .load(inputFile);
 
	// Print the schema of the DataFrame
	dfCsv.printSchema();
 
	// Print the number of lines in the DataFrame
	System.out.println("The DataFrame contains " + dfCsv.count() + "rows");
    }
}
