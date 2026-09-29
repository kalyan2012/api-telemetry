package com.analytics.ingest.repository;

import com.analytics.ingest.model.ApiLogDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ApiLogElasticRepository extends ElasticsearchRepository<ApiLogDocument, String> {
}