package org.example.Hospital;

import lombok.Data;

import java.io.Serializable;

@Data
public class Doctor implements Serializable {
    private Long id;
    private String firstName;
    private String lastName;
    private String phoneNumber;
    private String email;
    private String speciality;
}
