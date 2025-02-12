package com.filesender;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.List;

@SpringBootApplication
public class FileSenderApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(FileSenderApplication.class);
	private static final String FILE_PATH = "data.json";
	private static final String PROCESS_URL = "http://localhost:8081/process";

	public static void main(String[] args) {
		SpringApplication.run(FileSenderApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		logger.info("Starting file-based microservice communication");
		sendJsonData();
	}

	private void sendJsonData() throws IOException {
		RestTemplate restTemplate = new RestTemplate();
		ObjectMapper objectMapper = new ObjectMapper();

		File file = new File(FILE_PATH);
		if (!file.exists()) {
			logger.error("File {} does not exist!", FILE_PATH);
			return;
		}

		logger.info("Reading JSON file for processing request");
		List<Object> jsonData = objectMapper.readValue(file, new TypeReference<>() {});

		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", "application/json");
		HttpEntity<List<Object>> requestEntity = new HttpEntity<>(jsonData, headers);

		logger.info("Sending JSON data to Microservice B");
		long startTime = System.currentTimeMillis();
		ResponseEntity<String> response = restTemplate.postForEntity(PROCESS_URL, requestEntity, String.class);
		long endTime = System.currentTimeMillis();

		logger.info("Response from Microservice B: {}", response.getBody());
		logger.info("Total time taken for sending data: {} ms", (endTime - startTime));
	}
}
