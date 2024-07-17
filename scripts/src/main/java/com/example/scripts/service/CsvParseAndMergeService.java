package com.example.scripts.service;

import com.example.scripts.entity.Translation;
import com.example.scripts.model.Sentence;
import com.example.scripts.model.SentenceWithAudioFilename;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class CsvParseAndMergeService {

    public List<Translation> createRawData() throws IOException {
        ClassPathResource classPathResource = new ClassPathResource("sentences.csv");
        InputStream sentencesInputStream = classPathResource.getInputStream();

        ClassPathResource translationsClassPathResource = new ClassPathResource("links.csv");
        InputStream translationsInputStream = translationsClassPathResource.getInputStream();

        ClassPathResource audioUrlsClassPathResource = new ClassPathResource("sentences_with_audio.csv");
        InputStream audioUrlsInputStream = audioUrlsClassPathResource.getInputStream();

        Map<String, String> sentences = new HashMap<>();
        Map<String, String> translations = new HashMap<>();
        Map<String, String> audioUrls = new HashMap<>();

        // Load sentences
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(sentencesInputStream, StandardCharsets.UTF_8))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator('\t')
                        .withIgnoreQuotations(true)
                        .build())
                .build()) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String id = nextLine[0];
                String lang = nextLine[1];
                String text = nextLine[2];
                if (lang.equals("eng") || lang.equals("vie")) {
                    sentences.put(id, text);
                }
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
        System.out.println("sentences.size() = " + sentences.size());
        // Load links
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(translationsInputStream, StandardCharsets.UTF_8))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                .withSeparator('\t')
                .withIgnoreQuotations(true)
                .build()).build()
        ) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String engId = nextLine[0];
                String vieId = nextLine[1];
                if (sentences.containsKey(engId) && sentences.containsKey(vieId)) {
                    translations.put(engId, vieId);
                }
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
        System.out.println("translations.size() = " + translations.size());
        // Load audio URLs
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(audioUrlsInputStream, StandardCharsets.UTF_8)).withCSVParser(new com.opencsv.CSVParserBuilder()
                .withSeparator('\t')
                .withIgnoreQuotations(true)
                .build()).build()) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                String id = nextLine[0];
                String username = nextLine[1];
                String license = nextLine[2];
                String attributionUrl = nextLine[3];
                if (sentences.containsKey(id) && license != null && !license.isEmpty()) {
                    audioUrls.put(id, "https://audio.tatoeba.org/sentences/eng/" + id + ".mp3");
                }
            }
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
        System.out.println("audioUrls.size() = " + audioUrls.size());
        List<Translation> translationList = new ArrayList<>();
        for (Map.Entry<String, String> entry : translations.entrySet()) {
            String engId = entry.getKey();
            String vieId = entry.getValue();
            Translation translation = new Translation(
                    Integer.parseInt(engId),
                    sentences.get(engId),
                    audioUrls.getOrDefault(engId, ""),
                    Integer.parseInt(vieId),
                    sentences.get(vieId)
            );
            translationList.add(translation);

        }
        System.out.println("translationList.size() = " + translationList.size());
        return translationList;
    }

}
