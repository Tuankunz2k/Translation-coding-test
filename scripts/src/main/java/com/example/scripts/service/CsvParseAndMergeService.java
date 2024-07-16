package com.example.scripts.service;

import com.example.scripts.entity.Translation;
import com.example.scripts.model.LinkFileName;
import com.example.scripts.model.Sentence;
import com.example.scripts.model.SentenceWithAudioFilename;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
public class CsvParseAndMergeService {

    public List<Translation> createRawData() throws IOException {

        List<Sentence> sentences = this.readSentencesFromCSV();
        List<LinkFileName> linkFileNames = this.readLinksFileNameFromCSV(sentences);
        List<SentenceWithAudioFilename> sentencesWithAudioFilenames = this.readSentencesWithAudioFilenameFromCSV(sentences);

        var englishSentences = sentences.stream().filter(sentence -> Objects.equals(sentence.getLang(), "eng")).toList();
        var vietnameseSentences = sentences.stream().filter(sentence -> Objects.equals(sentence.getLang(), "vie")).toList();

        List<Translation> translationList = new ArrayList<>();
        for (Sentence englishSentence : englishSentences) {
            List<Long> collectTranslationId = linkFileNames
                    .stream()
                    .filter(linkFileName -> Objects.equals(englishSentence.getSentenceId(), linkFileName.getSentenceId()))
                    .map(LinkFileName::getTranslationId)
                    .toList();

            boolean sentenceAudioLicenseAvailable = sentencesWithAudioFilenames
                    .stream()
                    .anyMatch(sentencesWithAudio ->
                            Objects.equals(englishSentence.getSentenceId(), sentencesWithAudio.getSentenceId())
                                    && (!Objects.equals(sentencesWithAudio.getLicense(), "\\N"))
                    );

            for (Sentence vietnameseSentence : vietnameseSentences) {
                if (collectTranslationId.contains(vietnameseSentence.getSentenceId())) {
                    for (var translationId : collectTranslationId) {
                        if (Objects.equals(translationId, vietnameseSentence.getSentenceId())) {
                            translationList.add(getTranslation(englishSentence, vietnameseSentence, sentenceAudioLicenseAvailable));
                            break;
                        }
                    }
                }
            }

        }
        return translationList;
    }

    private Translation getTranslation(Sentence englishSentence, Sentence vietnameseSentence, boolean sentenceAvailable) {
        Translation entityTranslation = new Translation();
        entityTranslation.setId(englishSentence.getSentenceId());
        entityTranslation.setText(englishSentence.getText());
        entityTranslation.setAudioUrl(this.generateAudioUrl(sentenceAvailable, englishSentence));
        entityTranslation.setTranslateId(vietnameseSentence.getSentenceId());
        entityTranslation.setTranslateText(vietnameseSentence.getText());
        return entityTranslation;
    }

    private String generateAudioUrl(boolean sentenceAudioLicenseAvailable, Sentence sentence) {
        if (sentenceAudioLicenseAvailable){
            return  "https://audio.tatoeba.org/" + sentence.getLang() + "/" + sentence.getSentenceId() + ".mp3";
        }
        return null;
    }


    private List<Sentence> readSentencesFromCSV() throws IOException {
        List<Sentence> sentences = new ArrayList<>();
        ClassPathResource classPathResource = new ClassPathResource("sentences1.csv");
        InputStream inputStream = classPathResource.getInputStream();


        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator('\t')
                        .withIgnoreQuotations(true)
                        .build())
                .build()) {
            String[] record;
            while ((record = reader.readNext()) != null) {
                Sentence sentence = new Sentence();
                if (Objects.equals(record[1], "vie") || Objects.equals(record[1], "eng")) {
                    sentence.setSentenceId(Long.parseLong(record[0]));
                    sentence.setLang(record[1]);
                    sentence.setText(record[2]);
                    sentences.add(sentence);
                }
            }
            System.out.println("sentences.size() = " + sentences.size());
            return sentences;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sentences;
    }

    private List<LinkFileName> readLinksFileNameFromCSV(List<Sentence> sentences) throws IOException {
        List<LinkFileName> linkFileNames = new ArrayList<>();
        ClassPathResource classPathResource = new ClassPathResource("links1.csv");
        InputStream inputStream = classPathResource.getInputStream();
        List<Long> collectSentenceId = sentences.stream().map(Sentence::getSentenceId).toList();
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator('\t')
                        .withIgnoreQuotations(true)
                        .build())
                .build()) {
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (collectSentenceId.contains(Long.parseLong(record[0]))){
                    LinkFileName linkFileName = new LinkFileName();
                    linkFileName.setSentenceId(Long.parseLong(record[0]));
                    linkFileName.setTranslationId(Long.parseLong(record[1]));
                    linkFileNames.add(linkFileName);
                }
            }
            System.out.println("linkFileNames.size() = " + linkFileNames.size());
            return linkFileNames;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return linkFileNames;
    }

    private List<SentenceWithAudioFilename> readSentencesWithAudioFilenameFromCSV(List<Sentence> sentences) throws IOException {
        List<SentenceWithAudioFilename> sentencesWithAudioFilenames = new ArrayList<>();
        List<Long> collectSentenceId = sentences.stream().map(Sentence::getSentenceId).toList();
        ClassPathResource classPathResource = new ClassPathResource("sentences_with_audio1.csv");
        InputStream inputStream = classPathResource.getInputStream();
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .withCSVParser(new com.opencsv.CSVParserBuilder()
                        .withSeparator('\t')
                        .withIgnoreQuotations(true)
                        .build())
                .build()) {
            String[] record;
            while ((record = reader.readNext()) != null) {
                if (collectSentenceId.contains(Long.parseLong(record[0]))){
                    SentenceWithAudioFilename sentencesWithAudioFilename = new SentenceWithAudioFilename();
                    sentencesWithAudioFilename.setSentenceId(Long.parseLong(record[0]));
                    sentencesWithAudioFilename.setUserName(record[1]);
                    sentencesWithAudioFilename.setLicense(record[2]);
                    sentencesWithAudioFilename.setAttributionUrl(record[3]);
                    sentencesWithAudioFilenames.add(sentencesWithAudioFilename);
                }
            }
            System.out.println("sentencesWithAudioFilenames.size() : " + sentencesWithAudioFilenames.size());
            return sentencesWithAudioFilenames;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sentencesWithAudioFilenames;
    }
}
