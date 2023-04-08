package org.example.SparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class ApplicationFive {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkSession spark = SparkSession.builder().appName("Spark SQL").master("local[*]").getOrCreate();
        Dataset<Row> df = spark.read().option("multiline", true).json("src/main/resources/employees.json");
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        // convert Dataset<Row> to Dataset<Employee> : Dataframe to Dataset
        Dataset<Employee> ds = df.as(employeeEncoder);
        // second way to convert Dataset<Row> to Dataset<Employee> using map : Dataframe to Dataset
        Dataset<Employee> ds1 = df.map((MapFunction<Row, Employee>) row -> {
                    Employee employee = new Employee();
                    employee.setName(row.getString(0));
                    employee.setCity(row.getString(1));
                    employee.setAge(row.getInt(2));
                    employee.setSalary(row.getDouble(3));
                    return employee;
}, employeeEncoder);
        // convert Dataset<Employee> to JavaRDD<Employee> : Dataset to RDD
        JavaRDD<Employee> rdd = ds.toJavaRDD();
        // convert JavaRDD<Employee> to Dataset<Employee> : RDD to Dataset
        Dataset<Employee> ds2 = spark.createDataset(rdd.rdd(), employeeEncoder);
        // second way to convert JavaRDD<Employee> to Dataset<Employee> : RDD to Dataset
        Dataset<Employee> ds3 = spark.createDataset(rdd.rdd(), employeeEncoder);
        // convert Dataset<Employee> to Dataset<Row> : Dataset to Dataframe
        Dataset<Row> df2 = ds.toDF();
        // second way to convert Dataset<Employee> to Dataset<Row> using map : Dataset to Dataframe
        Dataset<Row> df3 = ds.map((MapFunction<Employee, Employee>) employee -> employee, employeeEncoder).toDF();




    }
}
