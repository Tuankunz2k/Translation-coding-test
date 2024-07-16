package com.example.translation.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Entity
@Getter
@Setter
@ToString
@RequiredArgsConstructor
public class Translation {
    @Id
    private Long id;
    private String text;
    private String audioUrl;
    private Long translateId;
    private String translateText;
}
