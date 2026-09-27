package analytics_engine.service;

import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.transaction.annotation.Transactional;

import analytics_engine.model.ApiLog;
import analytics_engine.model.Microservice;
import analytics_engine.repository.ApiLogRepository;
import jakarta.persistence.EntityManager;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class AnalyticsListener {
    private final ApiLogRepository apiLogRepository;
    private final ObjectMapper mapper = new ObjectMapper();
    private final EntityManager entityManager;
    private final StringRedisTemplate redisTemplate;

    public AnalyticsListener(ApiLogRepository apiLogRepository, EntityManager entityManager, StringRedisTemplate redisTemplate) {
        this.apiLogRepository = apiLogRepository;
        this.entityManager = entityManager;
        this.redisTemplate = redisTemplate;
    }

    @Transactional(rollbackFor = Exception.class)
    @KafkaListener(topics = "api-telemetry", groupId = "analytics-consumer-group")
    public void consume(List<String> messages) {
        long now = System.currentTimeMillis();
        List<ApiLog> batch = messages.stream()
        .map(message -> {
            try {
                JsonNode node = mapper.readTree(message);
                ApiLog log = new ApiLog();
                log.setTraceId(node.get("traceId").asString());
                log.setEndpoint(node.get("endpoint").asString());
                log.setHttpStatus(node.get("status").asInt());
                log.setLatency(node.get("latency").asDouble());
                log.setTimestamp(node.get("timestamp").asLong());

                String serviceName = node.get("serviceName").asString();
                Microservice serviceProxy = entityManager.getReference(Microservice.class, serviceName);
                log.setMicroservice(serviceProxy);

                return log;
            }
            catch(Exception e) {
                System.out.println("Error parsing message: " + message);
                return null;
            }
        })
        .filter(log -> log != null)
        .peek(log -> {
            try {
                String redisKey = "metrics:latency:" + log.getEndpoint();
                String redisValue = log.getTraceId() + ":" + log.getLatency();
                
                redisTemplate.opsForZSet().add(redisKey, redisValue, log.getTimestamp());
                redisTemplate.opsForZSet().removeRangeByScore(redisKey, 0, now - 300_000);
            }
            catch(Exception e) {
                System.out.println("Error processing message: " + log);
                e.printStackTrace();
            }
        })
        .toList();

        apiLogRepository.saveAll(batch);

        System.out.println("Processed " + batch.size() + " messages");
    }
}
