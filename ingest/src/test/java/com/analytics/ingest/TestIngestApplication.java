package com.analytics.ingest;

import org.springframework.boot.SpringApplication;

public class TestIngestApplication {

	public static void main(String[] args) {
		SpringApplication.from(IngestApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
