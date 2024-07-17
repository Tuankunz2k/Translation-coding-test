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

//    public List<Translation> createRawData() throws IOException {
//
//        List<Sentence> sentences = this.readSentencesFromCSV();
//        Map<Integer, List<Integer>> linkFileNames = this.readLinksFileNameFromCSV(sentences);
//        List<SentenceWithAudioFilename> sentencesWithAudioFilenames = this.readSentencesWithAudioFilenameFromCSV(sentences);
//
//        var englishSentences = sentences.stream().filter(sentence -> Objects.equals(sentence.getLang(), "eng")).toList();
//        var vietnameseSentences = sentences.stream().filter(sentence -> Objects.equals(sentence.getLang(), "vie")).toList();
//
//        List<Translation> translationList = new ArrayList<>();
//        for (Sentence englishSentence : englishSentences) {
//            List<Integer> translationIdList = linkFileNames.getOrDefault(englishSentence.getSentenceId(), Collections.emptyList());
//
//            for (Sentence vietnameseSentence : vietnameseSentences) {
//                if (translationIdList.contains(vietnameseSentence.getSentenceId())) {
//                    for (var translationId : translationIdList) {
//                        if (Objects.equals(translationId, vietnameseSentence.getSentenceId())) {
//                            translationList.add(getTranslation(englishSentence, vietnameseSentence, sentencesWithAudioFilenames));
//                        }
//                    }
//                }
//            }
//
//        }
//        return translationList;
//    }

//    private Translation getTranslation(Sentence englishSentence, Sentence vietnameseSentence, List<SentenceWithAudioFilename> sentencesWithAudioFilenames) {
//        Translation entityTranslation = new Translation();
//        entityTranslation.setId(englishSentence.getSentenceId());
//        entityTranslation.setText(englishSentence.getText());
//        entityTranslation.setAudioUrl(this.generateAudioUrl(sentencesWithAudioFilenames, englishSentence));
//        entityTranslation.setTranslateId(vietnameseSentence.getSentenceId());
//        entityTranslation.setTranslateText(vietnameseSentence.getText());
//        return entityTranslation;
//    }
//
//    private String generateAudioUrl(List<SentenceWithAudioFilename> sentencesWithAudioFilenames, Sentence sentence) {
//        boolean sentenceAudioLicenseAvailable = sentencesWithAudioFilenames
//                .stream()
//                .anyMatch(sentencesWithAudio ->
//                        Objects.equals(sentence.getSentenceId(), sentencesWithAudio.getSentenceId())
//                                && (!Objects.equals(sentencesWithAudio.getLicense(), "\\N"))
//                );
//        if (sentenceAudioLicenseAvailable) {
//            return "https://audio.tatoeba.org/" + sentence.getLang() + "/" + sentence.getSentenceId() + ".mp3";
//        }
//        return null;
//    }
//
//
//    private List<Sentence> readSentencesFromCSV() throws IOException {
//        List<Sentence> sentences = new ArrayList<>();
//        ClassPathResource classPathResource = new ClassPathResource("sentences1.csv");
//        InputStream inputStream = classPathResource.getInputStream();
//
//        int numberOfThreads = 5;
//        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
//
//        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
//                .withCSVParser(new com.opencsv.CSVParserBuilder()
//                        .withSeparator('\t')
//                        .withIgnoreQuotations(true)
//                        .build())
//                .build()) {
//
//            String[] record;
//            while ((record = reader.readNext()) != null) {
//                executorService.submit(new CsvLineSentencesProcessor(record, sentences));
//            }
//            System.out.println("sentences.size() = " + sentences.size());
//            return sentences;
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            executorService.shutdown();
//            try {
//                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//                    executorService.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                executorService.shutdownNow();
//            }
//        }
//        return sentences;
//    }
//
//    private Map<Integer,List<Integer>> readLinksFileNameFromCSV(List<Sentence> sentences) throws IOException {
//        Map<Integer,List<Integer>> linkFileNames = new HashMap<>();
//        var collectSentenceId = this.getSentenceIds(sentences);
//        ClassPathResource classPathResource = new ClassPathResource("links1.csv");
//        InputStream inputStream = classPathResource.getInputStream();
//        int numberOfThreads = 5;
//        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
//        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
//                .withCSVParser(new com.opencsv.CSVParserBuilder()
//                        .withSeparator('\t')
//                        .withIgnoreQuotations(true)
//                        .build())
//                .build()) {
//            String[] record;
//            while ((record = reader.readNext()) != null) {
//                executorService.submit(new CsvLineLinkFileNameProcessor(record, linkFileNames, collectSentenceId));
//            }
//            System.out.println("linkFileNames.size() = " + linkFileNames.size());
//            return linkFileNames;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return linkFileNames;
//    }
//
//    private List<SentenceWithAudioFilename> readSentencesWithAudioFilenameFromCSV(List<Sentence> sentences) throws IOException {
//        List<SentenceWithAudioFilename> sentencesWithAudioFilenames = new ArrayList<>();
//        var collectSentenceId = this.getSentenceIds(sentences);
//        ClassPathResource classPathResource = new ClassPathResource("sentences_with_audio1.csv");
//        InputStream inputStream = classPathResource.getInputStream();
//        int numberOfThreads = 5;
//        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);
//        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
//                .withCSVParser(new com.opencsv.CSVParserBuilder()
//                        .withSeparator('\t')
//                        .withIgnoreQuotations(true)
//                        .build())
//                .build()) {
//            String[] record;
//            while ((record = reader.readNext()) != null) {
//                executorService.submit(new CsvLineSentencesWithAudioFilenameProcessor(record, sentencesWithAudioFilenames, collectSentenceId));
//            }
//            System.out.println("sentencesWithAudioFilenames.size() : " + sentencesWithAudioFilenames.size());
//            return sentencesWithAudioFilenames;
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            executorService.shutdown();
//            try {
//                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//                    executorService.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                executorService.shutdownNow();
//            }
//        }
//        return sentencesWithAudioFilenames;
//    }
//
//    static class CsvLineSentencesProcessor implements Runnable {
//        private final String[] record;
//        private final List<Sentence> sentences;
//
//        public CsvLineSentencesProcessor(String[] record, List<Sentence> sentences) {
//            this.record = record;
//            this.sentences = sentences;
//        }
//
//        @Override
//        public void run() {
//            Sentence sentence = new Sentence();
//            if (Objects.equals(record[1], "vie") || Objects.equals(record[1], "eng")) {
//                sentence.setSentenceId(Integer.parseInt(record[0]));
//                sentence.setLang(record[1]);
//                sentence.setText(record[2]);
//                sentences.add(sentence);
//            }
//        }
//    }
//
//    static class CsvLineLinkFileNameProcessor implements Runnable {
//        private final String[] record;
//        private final Map<Integer, List<Integer>> linkFileNames;
//        private final List<Integer> collectSentenceId;
//
//        public CsvLineLinkFileNameProcessor(String[] record,
//                                            Map<Integer, List<Integer>> linkFileNames,
//                                            List<Integer> collectSentenceId
//        ) {
//            this.record = record;
//            this.linkFileNames = linkFileNames;
//            this.collectSentenceId = collectSentenceId;
//        }
//
//        @Override
//        public void run() {
//            if (collectSentenceId.contains(Integer.parseInt(record[0]))) {
//
//                int sentenceId = Integer.parseInt(record[0].trim());
//                int translationId = Integer.parseInt(record[1].trim());
//                linkFileNames.computeIfAbsent(sentenceId, k -> new ArrayList<>()).add(translationId);
//                linkFileNames.computeIfAbsent(translationId, k -> new ArrayList<>()).add(sentenceId);
//            }
//        }
//    }
//
//    static class CsvLineSentencesWithAudioFilenameProcessor implements Runnable {
//        private final String[] record;
//        private final List<SentenceWithAudioFilename> sentencesWithAudioFilenames;
//        private final List<Integer> collectSentenceId;
//
//        public CsvLineSentencesWithAudioFilenameProcessor(String[] record,
//                                                          List<SentenceWithAudioFilename> sentencesWithAudioFilenames,
//                                                          List<Integer> collectSentenceId
//        ) {
//            this.record = record;
//            this.sentencesWithAudioFilenames = sentencesWithAudioFilenames;
//            this.collectSentenceId = collectSentenceId;
//        }
//
//        @Override
//        public void run() {
//            if (collectSentenceId.contains(Integer.parseInt(record[0]))) {
//                SentenceWithAudioFilename sentencesWithAudioFilename = new SentenceWithAudioFilename();
//                sentencesWithAudioFilename.setSentenceId(Integer.parseInt(record[0]));
//                sentencesWithAudioFilename.setUserName(record[1]);
//                sentencesWithAudioFilename.setLicense(record[2]);
//                sentencesWithAudioFilename.setAttributionUrl(record[3]);
//                sentencesWithAudioFilenames.add(sentencesWithAudioFilename);
//            }
//        }
//    }
//
//    private List<Integer> getSentenceIds(List<Sentence> sentences) {
//        List<Integer> sentenceIds = new ArrayList<>();
//        for (var sentence : sentences) {
//            if (sentence != null) {
//                sentenceIds.add(sentence.getSentenceId());
//            }
//        }
//        return sentenceIds;
//    }

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
