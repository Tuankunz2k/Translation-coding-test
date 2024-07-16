package com.example.scripts.entity;

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
    private Long id;
    private String text;
    private String audioUrl;
    private Long translateId;
    private String translateText;
}
