package org.example.SparkSQL;

import lombok.Data;

import java.io.Serializable;
@Data
public class Employee implements Serializable {
    private long id;
    private String name;
    private long age;
    private String city;
    private Double salary;

}
