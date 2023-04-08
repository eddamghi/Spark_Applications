package org.example.Hospital.firstExercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;


public class FirstQuestion {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = sparkSession.read()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/db_hopital")
                .option("user","root")
                .option("password","")
                .option("dbtable", "doctors")
                .option("dbtable", "consultations")
//               Using SQL query to select consultation grouped by day
                .option("query", "select date, count(*) as count from consultations group by date")
                .load();
        df.show();
                // Using dataframes
        Dataset<Row> dfGrouped = df.groupBy("date").count().distinct();
        dfGrouped.show();

    }
}
