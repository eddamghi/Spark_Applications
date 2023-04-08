package org.example.SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVApplication {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/employees.csv");
        df.printSchema();
        df.show();
    }
}
