package org.example.Hospital;

import lombok.Data;

import java.io.Serializable;
import java.sql.Date;

@Data
public class Patient implements Serializable {
    private Long id;
    private String firstName;
    private String lastName;
    private Date birthDate;
    private String CIN;
    private String phoneNumber;
    private String email;
}
