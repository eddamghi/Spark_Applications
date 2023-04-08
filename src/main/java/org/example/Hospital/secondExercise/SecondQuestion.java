package org.example.Hospital.secondExercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class SecondQuestion {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = sparkSession.read().option("header", true).option("inferSchema", true).csv("incidents.csv");
        // count the number of incidents per year
        Dataset<Row> incidentsPerYear = df
                .withColumn("year", functions.year(functions.col("date")))
                .groupBy("year")
                .count()
                .withColumnRenamed("count", "incidents");
        incidentsPerYear.show();
        // display the two years with the highest number of incidents
        Dataset<Row> twoYearsWithHighestIncidents = incidentsPerYear.orderBy(functions.desc("incidents")).limit(2);
        twoYearsWithHighestIncidents.show();

    }
}
