package org.example.Hospital;

import lombok.Data;

import java.io.Serializable;
import java.sql.Date;

@Data
public class Consultation implements Serializable {
    private Long id;
    private Patient patient;
    private Doctor doctor;
    private Date date;
}
