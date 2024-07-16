package com.example.scripts.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sentence {
    private Integer sentenceId;
    private String lang;
    private String text;
}
