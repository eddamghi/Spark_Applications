package org.example.Hospital.firstExercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

public class ThirdQuestion {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = sparkSession.read()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/db_hopital")
                .option("user","root")
                .option("password","")
                // display for each doctor the number of patients assisted the format should be : lastName | firstName | number of patients
                .option("query", "select doctors.lastName, doctors.firstName, count(*) as patients from consultations inner join doctors on consultations.doctor = doctors.id group by doctors.lastName, doctors.firstName")
                .load();
        df.show();
    }
}

