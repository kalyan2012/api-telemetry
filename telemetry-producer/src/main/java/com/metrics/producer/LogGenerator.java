package com.metrics.producer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class LogGenerator {
    private static final String TOPIC = "api-telemetry";
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final AtomicLong counter = new AtomicLong(0);

    public static ApiLogEvent generateLog() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int chance = random.nextInt(0, 100);
        String endpoint;
        int status;
        double latency;

        if (chance < 60) {
            endpoint = "/api/v1/products";
            status = (random.nextInt(100) < 99) ? 200 : 500;
            latency = 15.0 + (random.nextDouble() * 25.0);
        } else if (chance < 85) {
            endpoint = "/api/v1/search";
            status = (random.nextInt(100) < 97) ? 200 : 500;
            latency = 80.0 + (random.nextDouble() * 170.0);
        } else if (chance < 95) {
            endpoint = "/api/v1/checkout";
            status = (random.nextInt(100) < 92) ? 200 : 500;
            latency = (random.nextInt(100) < 5) ? 800.0 + (random.nextDouble() * 400.0) : // SLA Breach spike
                    150.0 + (random.nextDouble() * 450.0);
        } else {
            endpoint = "/api/v1/legacy-endpoint"; // Unmonitored route
            status = 404;
            latency = 5.0 + (random.nextDouble() * 10.0);
        }

        return new ApiLogEvent(
                UUID.randomUUID().toString(),
                "api-gateway",
                endpoint,
                status,
                Math.round(latency * 100.0) / 100.0,
                System.currentTimeMillis());
    }

    private static class ProducerRunnable implements Runnable {
        private final KafkaProducer<String, String> producer;

        public ProducerRunnable(KafkaProducer<String, String> producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            try {
                ApiLogEvent event = generateLog();
                String json = mapper.writeValueAsString(event);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, event.endpoint(), json);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("Failed to send message: " + exception.getMessage());
                    } else {
                        counter.incrementAndGet();
                        System.out.println("Message sent: " + metadata.toString());
                    }
                });
            } catch (Exception e) {
                System.out.println("Failed to send message: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        System.out.println("Starting Telemetry Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Runnable runnable = new ProducerRunnable(producer);

        scheduler.scheduleAtFixedRate(runnable, 0, 10, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(() -> {
            long count = counter.getAndSet(0);
            System.out.println("Produced " + count + " messages in the last 5 seconds.");
        }, 5, 5, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Cleaning up...");

            scheduler.shutdown();

            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Scheduler didn't exit cleanly. Forcing shutdown...");
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
            }
            System.out.println("Closing Kafka Producer...");
            // producer.close();
            System.out.println("Shutdown complete.");
        }));
    }
}
