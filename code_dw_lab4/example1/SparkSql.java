package example1;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import java.util.Arrays;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkSql {
    public static void main(String[] args) throws Exception {
	// Create Spark Session
        SparkSession spark = SparkSession
            .builder()
            .appName("Java SparkSQL Example")
            .getOrCreate();

	// Load the bookings table into a Spark Dataset
        Dataset<Row> jdbcDF = spark.read()
            .format("jdbc")
            .option("url", "jdbc:postgresql://master:5432/clubdata")
            .option("dbtable", "cd.bookings")
            .option("user", "student")
            .option("password", "student")
            .load();
	
	// Print the schema
	jdbcDF.printSchema();

	// Print the number of rows
	System.out.println(jdbcDF.count());

	// Close the Spark Session
	spark.stop();

    };
};
