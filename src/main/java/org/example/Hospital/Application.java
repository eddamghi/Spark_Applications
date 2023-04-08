package org.example.Hospital;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;

public class Application {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession sparkSession = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Encoder<Patient> patientEncoder = Encoders.bean(Patient.class);
        Encoder<Consultation> consultationEncoder = Encoders.bean(Consultation.class);
        Encoder<Doctor> doctorEncoder = Encoders.bean(Doctor.class);
        Dataset<Row> df = sparkSession.read()
                .format("jdbc")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("url","jdbc:mysql://localhost:3306/db_hopital")
                .option("user","root")
                .option("password","")
//                .option("dbtable", "patients")
                .option("dbtable", "doctors")
                .option("dbtable", "consultations")
                // first question
                // Using SQL query to select consultation grouped by day
//             .option("query", "select date, count(*) as count from consultations group by date")
//                .load();
//        df.show();
        // Using dataframes
//        Dataset<Row> dfGrouped = df.groupBy("date").count().distinct();
//        dfGrouped.show();

        // second question
        // Using SQL query to display number of consultations by doctor the format should be : lastName | firstName | number of consultations
//             .option("query", "select doctors.lastName, doctors.firstName, count(*) as count from consultations inner join doctors on consultations.doctor = doctors.id group by doctors.lastName, doctors.firstName")
             .load();
//        df.show();

            // Using dataframes
        Dataset<Row> dfGrouped = df.groupBy("id", "lastName", "firstName").count();
//        Dataset<Row> consultationDF = df.groupBy("consultations.id", "consultations.date", "consultations.doctor").count();
        Dataset<Row> joinDF = df.join(df, dfGrouped.col("id").equalTo(df.col("doctor")));
        joinDF.show();

//        Dataset<Row> doctorDF = df.groupBy("doctor.id", "doctor.lastName", "doctor.firstName").count();
//        Dataset<Row> consultationDF = df.groupBy("consultations.id", "consultations.date", "consultations.doctor").count();
//        consultationDF.join(doctorDF,"doctor.id").show();



//                // display for each doctor the number of patients assisted the format should be : lastName | firstName | number of patients
//                .option("query", "select doctors.lastName, doctors.firstName, count(*) as count from consultations inner join doctors on consultations.doctor = doctors.id group by doctors.lastName, doctors.firstName")
//                .load();



    }
}
