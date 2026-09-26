package analytics_engine.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import analytics_engine.model.Microservice;

@Repository 
public interface MicroserviceRepository extends JpaRepository<Microservice, String> {
    
}
