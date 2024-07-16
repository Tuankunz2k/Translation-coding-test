package com.example.translation.controller;

import com.example.translation.entity.Translation;
import com.example.translation.service.TranslationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController("/translations")
public class TranslationController {
    @Autowired
    private final TranslationService translationService;

    public TranslationController(TranslationService translationService) {
        this.translationService = translationService;
    }

    @GetMapping("")
    public ResponseEntity<?> translate(@Validated @PathVariable Integer pageNumber, @PathVariable Integer pageSize) {
        Page<Translation> translationPage = translationService.getTranslationPage(pageNumber, pageSize);
        return ResponseEntity.ok().body(translationPage);
    }
}
