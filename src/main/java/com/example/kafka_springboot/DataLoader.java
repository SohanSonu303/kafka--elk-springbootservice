package com.example.kafka_springboot;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/load-test")
public class DataLoader {

    private final LoadProducerService loadProducerService;


    public DataLoader(LoadProducerService loadProducerService) {
        this.loadProducerService = loadProducerService;
    }

    @GetMapping("/{count}")
        public String sendLoad(@PathVariable int count) {
            return loadProducerService.produceMessages(count);
        }
    }