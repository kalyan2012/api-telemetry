package analytics_engine.model;

import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "api_logs")
public class ApiLog {

    @Id
    private String traceId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "service_name")
    private Microservice microservice;

    private String endpoint;
    private Integer httpStatus;
    private Double latency;
    private Long timestamp;

    public ApiLog(String traceId, Microservice microservice, String endpoint, Integer httpStatus, Double latency, Long timestamp) {
        this.traceId = traceId;
        this.microservice = microservice;
        this.endpoint = endpoint;
        this.httpStatus = httpStatus;
        this.latency = latency;
        this.timestamp = timestamp;
    }

    public ApiLog() {
        //TODO Auto-generated constructor stub
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public Microservice getMicroservice() {
        return microservice;
    }

    public void setMicroservice(Microservice microservice) {
        this.microservice = microservice;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }

    public void setHttpStatus(Integer httpStatus) {
        this.httpStatus = httpStatus;
    }

    public Double getLatency() {
        return latency;
    }

    public void setLatency(Double latency) {
        this.latency = latency;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
