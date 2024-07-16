package com.example.translation.service;

import com.example.translation.entity.Translation;
import com.example.translation.repository.TranslationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class TranslationServiceImpl implements TranslationService {
    @Autowired
    private final TranslationRepository translationRepository;

    @Override
    public Page<Translation> getTranslationPage(Integer pageNum, Integer pageSize) {
        int offset = Optional.of(pageNum).map(p -> {
            if (p > 0) return p - 1;
            else return 0;
        }).orElse(0);
        Pageable pageable = PageRequest.of(offset, pageSize);
        return translationRepository.findAll(pageable);
    }
}
