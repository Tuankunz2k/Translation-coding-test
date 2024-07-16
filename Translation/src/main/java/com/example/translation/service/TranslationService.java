package com.example.translation.service;

import com.example.translation.entity.Translation;
import org.springframework.data.domain.Page;
public interface TranslationService {
    Page<Translation> getTranslationPage(Integer pageNum, Integer pageSize);
}
