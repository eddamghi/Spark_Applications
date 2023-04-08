package org.example.SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONApplication {
    public static void main(String[] args) throws AnalysisException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.read().option("multiline", true).json("src/main/resources/employees.json");
//        df.printSchema();
//        df.show();
//        df.select("name","city").show();
//        df.select(col("name"),col("city"), col("age").minus(10).alias("salary")).show();
//        df.filter(col("age").gt(20).and(col("city").startsWith("N"))).show();
//        df.filter("age > 20 and city like 'N%'").show();
//          df.createTempView("employees");
//          spark.sql("select * from employees where age > 20 and city like 'N%'").show();
        // display the average salary of employees in each city
//        spark.sql("select city, avg(salary) from employees group by city").show();
        df.groupBy("city").avg("salary").show();

    }
}
