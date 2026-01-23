package com.example.kafka_springboot;

import com.example.kafka_springboot.MessageBuffer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MessageConsumer {

    private final MessageBuffer buffer;

    public MessageConsumer(MessageBuffer buffer) {
        this.buffer = buffer;
    }

    @KafkaListener(topics = "demo-topic", groupId = "kafka-group")
    public void listen(String message) {
        buffer.add(message);
        System.out.println("Buffered: " + message);
    }
}
