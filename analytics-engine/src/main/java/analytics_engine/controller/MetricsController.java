package analytics_engine.controller;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;

@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private final StringRedisTemplate redisTemplate;

    public MetricsController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/p99")
    public ResponseEntity<Double> getP99Latency(@RequestParam String endpoint) {
        String redisKey = "metrics:latency:" + endpoint;

        Set<String> resultSet = redisTemplate.opsForZSet().range(redisKey, 0, -1);

        if (resultSet == null || resultSet.isEmpty()) {
            return ResponseEntity.ok(0.0);
        }

        List<Double> latencies = resultSet.stream()
                .map(value -> {
                    String[] parts = value.split(":");
                    return Double.parseDouble(parts[1]);
                })
                .sorted()
                .toList();

        int p99Index = (int) Math.ceil(latencies.size() * 0.99) - 1;
        p99Index = Math.max(0, p99Index);
        Double p99Latency = latencies.get(p99Index);

        return ResponseEntity.ok(p99Latency);
    }
}