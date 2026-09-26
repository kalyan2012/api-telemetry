package com.metrics.producer;

public record ApiLogEvent(String traceId,String serviceName,String endpoint,int httpStatus,double responseTimeMs,long timestamp){}