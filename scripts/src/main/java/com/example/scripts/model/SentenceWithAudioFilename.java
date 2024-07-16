package com.example.scripts.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SentenceWithAudioFilename {
    private Integer sentenceId;
    private String userName;
    private String license;
    private String attributionUrl;
}
