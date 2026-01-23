package com.example.kafka_springboot;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class LoadProducerService {

    public String produceMessages(int count) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768"); // 32KB

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < count; i++) {
            String msg = "{\"id\":" + i + ",\"event\":\"load-test\",\"value\":" + (int)(Math.random()*1000) + "}";

            producer.send(new ProducerRecord<>("demo-topic", msg));

            if (i % 10000 == 0) {
                System.out.println("Sent: " + i);
            }
        }

        producer.flush();
        producer.close();

        return "Successfully produced " + count + " messages.";
    }
}
