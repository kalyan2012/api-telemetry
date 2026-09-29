package com.analytics.ingest.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor     
@AllArgsConstructor
@Document(indexName = "api-telemetry")
public class ApiLogDocument {
    
    @Id
    private String traceId;
    private String endpoint;
    private int status;
    private double latency;
    private long timestamp;
    private String serviceName;
}
