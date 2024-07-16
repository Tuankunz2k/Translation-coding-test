package com.example.scripts.repository;

import com.example.scripts.entity.Translation;
import org.springframework.data.jpa.repository.JpaRepository;

@org.springframework.stereotype.Repository
public interface TranslationRepository extends JpaRepository<Translation, Long> {
}
