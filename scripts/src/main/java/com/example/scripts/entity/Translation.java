package com.example.scripts.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class Translation {
    @Id
    private Integer id;
    @Column(length = 5000)
    private String text;
    @Column(length = 5000)
    private String audioUrl;
    private Integer translateId;
    @Column(length = 5000)
    private String translateText;
}
