package org.example.Hospital.secondExercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FirstQuestion {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).csv("incidents.csv");
        df.groupBy("service").count().withColumnRenamed("count", "incidents").show();
    }
}
