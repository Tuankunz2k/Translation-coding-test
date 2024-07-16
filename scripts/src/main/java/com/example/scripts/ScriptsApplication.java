package com.example.scripts;

import com.example.scripts.service.ImportDatabaseService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;

@SpringBootApplication
public class ScriptsApplication {
    @Autowired
    private final ImportDatabaseService importDatabaseService;

    public ScriptsApplication(ImportDatabaseService importDatabaseService) {
        this.importDatabaseService = importDatabaseService;
    }

    public static void main(String[] args) {
        SpringApplication.run(ScriptsApplication.class, args);
    }

    @PostConstruct
    public void init() throws IOException {
        importDatabaseService.importData();
    }
}
