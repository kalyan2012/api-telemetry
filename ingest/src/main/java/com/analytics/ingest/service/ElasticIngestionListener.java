package com.analytics.ingest.service;


import com.analytics.ingest.model.ApiLogDocument;
import com.analytics.ingest.repository.ApiLogElasticRepository;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class ElasticIngestionListener {

    private final ApiLogElasticRepository repository;
    private final ObjectMapper mapper = new ObjectMapper();

    public ElasticIngestionListener(ApiLogElasticRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "api-telemetry", groupId = "elastic-ingestion-group-v2")
    public void consumeForElastic(List<String> messages) {

        List<ApiLogDocument> batch = messages.stream()
                .map(json -> {
                    try {
                        JsonNode node = mapper.readTree(json);

                        ApiLogDocument log = new ApiLogDocument();
                        log.setTraceId(node.get("traceId").asString());
                        log.setEndpoint(node.get("endpoint").asString());
                        log.setStatus(node.get("httpStatus").asInt());
                        log.setLatency(node.get("responseTimeMs").asDouble());
                        log.setTimestamp(node.get("timestamp").asLong());
                        log.setServiceName(node.get("serviceName").asString());

                        return log;
                    } catch (Exception e) {
                        System.err.println("Parse error: " + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();

        if (!batch.isEmpty()) {
            repository.saveAll(batch);
            System.out.println("Ingested " + batch.size() + " logs to Elasticsearch.");
        }
    }
}
