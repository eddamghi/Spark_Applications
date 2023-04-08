package org.example.SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.HashMap;
import java.util.Map;

public class SqlApplication {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Map<String, String> options = new HashMap<>();
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("url", "jdbc:mysql://localhost:3306/Spark_SQL");
        options.put("user", "root");
        options.put("password", "");

        Dataset<Row> df = spark.read().format("jdbc")
                .options(options)
                .option("dbtable", "employees")
                .option("query", "select * from employees where age > 20 and city like 'N%'")
                .load();
        df.printSchema();
        df.show();
}
}
