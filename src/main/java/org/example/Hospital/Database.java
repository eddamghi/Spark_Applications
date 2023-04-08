package org.example.Hospital;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class Database {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/DB_HOPITAL?createDatabaseIfNotExist=true&useSSL=true");
        options.put("user", "root");
        options.put("password", "");
        Dataset<Row> df = spark.read().format("jdbc")
                .options(options)
                .option("dbtable", "patients")
                .option("dbtable", "doctors")
                .option("dbtable", "consultations")
                .load();
        df.printSchema();
        df.show();
    }
}
