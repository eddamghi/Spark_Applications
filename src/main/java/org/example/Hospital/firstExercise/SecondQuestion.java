package org.example.Hospital.firstExercise;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;


public class SecondQuestion {
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
                .load();
        Dataset<Row> doctorDF = df.groupBy("id", "lastName", "firstName").df();
        Dataset<Row> df1 = sparkSession.read()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/db_hopital")
                .option("user","root")
                .option("password","")
                .option("dbtable", "consultations")
                .load();
        Dataset<Row> consultationDF = df1.groupBy("id", "doctor").df();
        Dataset<Row> joinedDF = consultationDF.join(doctorDF, consultationDF.col("doctor").equalTo(doctorDF.col("id")));
        Dataset<Row> resultDF = joinedDF.groupBy("lastName", "firstName").count().withColumnRenamed("count", "consultations");
        resultDF.show();
// Using SQL query
        Dataset<Row> SqlDF = sparkSession.read()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/db_hopital")
                .option("user","root")
                .option("password","")
                .option("query", "select doctors.lastName, doctors.firstName, count(*) as consultations from consultations inner join doctors on consultations.doctor = doctors.id group by doctors.lastName, doctors.firstName")
                .load();
        SqlDF.show();






    }
}

