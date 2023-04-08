package org.example.SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class ApplicationFour {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Employee> ds = spark.read().option("multiline", true).json("src/main/resources/employees.json").as(employeeEncoder);
//        ds.printSchema();
//        ds.show();
        ds.filter((FilterFunction<Employee>) employee -> employee.getAge() > 20 && employee.getCity().startsWith("N")).show();
        ds.filter("age > 20 and city like 'N%'").show();
    }
}
