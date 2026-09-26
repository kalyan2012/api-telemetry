package analytics_engine.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import analytics_engine.model.ApiLog;

@Repository 
public interface ApiLogRepository extends JpaRepository<ApiLog, String> {
    
}
