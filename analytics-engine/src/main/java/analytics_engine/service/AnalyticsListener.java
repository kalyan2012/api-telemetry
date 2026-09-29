package analytics_engine.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.transaction.annotation.Transactional;

import analytics_engine.model.ApiLog;
import analytics_engine.model.Microservice;
import analytics_engine.repository.ApiLogRepository;
import analytics_engine.repository.MicroserviceRepository;
import jakarta.persistence.EntityManager;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

@Service
public class AnalyticsListener {
    private final Set<String> seenServices = ConcurrentHashMap.newKeySet();
    private final ApiLogRepository apiLogRepository;
    private final MicroserviceRepository microserviceRepository;
    private final ObjectMapper mapper = new ObjectMapper();
    private final EntityManager entityManager;
    private final StringRedisTemplate redisTemplate;

    public AnalyticsListener(ApiLogRepository apiLogRepository, MicroserviceRepository microserviceRepository,
            EntityManager entityManager, StringRedisTemplate redisTemplate) {
        this.apiLogRepository = apiLogRepository;
        this.microserviceRepository = microserviceRepository;
        this.entityManager = entityManager;
        this.redisTemplate = redisTemplate;
    }

    @Transactional(rollbackFor = Exception.class)
    @KafkaListener(topics = "api-telemetry", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(List<String> messages) {
        System.out.println("Started consuming logs...");
        long now = System.currentTimeMillis();
        List<ApiLog> batch = messages.stream()
                .map(message -> {
                    try {
                        JsonNode node = mapper.readTree(message);
                        ApiLog log = new ApiLog();
                        log.setTraceId(node.get("traceId").asString());
                        log.setEndpoint(node.get("endpoint").asString());
                        log.setHttpStatus(node.get("httpStatus").asInt());
                        log.setLatency(node.get("responseTimeMs").asDouble());
                        log.setTimestamp(node.get("timestamp").asLong());

                        String serviceName = node.get("serviceName").asString();
                        if (!seenServices.contains(serviceName)) {
                            if (!microserviceRepository.existsById(serviceName)) {
                                microserviceRepository.save(new Microservice(serviceName));
                            }
                            seenServices.add(serviceName);
                        }

                        Microservice serviceProxy = entityManager.getReference(Microservice.class, serviceName);
                        log.setMicroservice(serviceProxy);

                        return log;
                    } catch (Exception e) {
                        System.out.println("Error parsing message: " + message);
                        System.out.println(e.getMessage());
                        return null;
                    }
                })
                .filter(log -> log != null)
                .peek(log -> {
                    // try {
                    // String redisKey = "metrics:latency:" + log.getEndpoint();
                    // String redisValue = log.getTraceId() + ":" + log.getLatency();

                    // redisTemplate.opsForZSet().add(redisKey, redisValue, log.getTimestamp());
                    // redisTemplate.opsForZSet().removeRangeByScore(redisKey, 0, now - 300_000);
                    // } catch (Exception e) {
                    // // System.out.println("Error processing message: " + log);
                    // // e.printStackTrace();
                    // }
                })
                .toList();

        apiLogRepository.saveAll(batch);
        apiLogRepository.flush();
        System.out.println("Processed " + batch.size() + " messages");

        long dbCount = apiLogRepository.count();
        System.out.println("DEBUG: The api_logs table currently has " + dbCount + " total rows in the database.");

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }
    }
}
