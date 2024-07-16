package com.example.scripts.service;

import com.example.scripts.entity.Translation;
import com.example.scripts.repository.TranslationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ImportDatabaseService {
    private final CsvParseAndMergeService csvParseAndMergeService;
    private final TranslationRepository translationRepository;

    @Transactional
    public void importData() {
        try {
            List<Translation> rawData = csvParseAndMergeService.createRawData();
            if (!rawData.isEmpty()){
                translationRepository.saveAll(rawData);
            }
        } catch (Exception e) {
            e.printStackTrace();
            // Handle exception
        }
    }
}
