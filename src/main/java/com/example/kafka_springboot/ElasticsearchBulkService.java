package com.example.kafka_springboot;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.example.kafka_springboot.MessageBuffer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ElasticsearchBulkService {

    private final MessageBuffer buffer;
    private final ElasticsearchClient esClient;

    public ElasticsearchBulkService(MessageBuffer buffer,
                                    ElasticsearchClient esClient) {
        this.buffer = buffer;
        this.esClient = esClient;
    }

    @Scheduled(fixedRate = 5000) // run every 5 seconds
    public void flushIfNeeded() throws Exception {
        flush();
    }

    @Scheduled(fixedRate  = 1000)
    public void checkFlushForSize() throws Exception{
        if(buffer.size() > 500){
            flush();
        }
    }

    private void flush() throws Exception {
        List<String> messages = buffer.drainAll();

        List<BulkOperation> bulkOps = messages.stream()
                .map(msg -> BulkOperation.of(b -> b.index(idx -> idx
                        .index("kafka-events")
                        .document(Map.of(
                                "message", msg,
                                "timestamp", System.currentTimeMillis()
                        ))
                )))
                .collect(Collectors.toList());

        BulkRequest bulkRequest = BulkRequest.of(
                req -> req.operations(bulkOps)
        );

        esClient.bulk(bulkRequest);

        System.out.println("🔥 Bulk flushed " + messages.size() + " messages to Elasticsearch");
    }
}

