package com.filesender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;

@SpringBootApplication
public class FileSenderApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(FileSenderApplication.class);
	private static final String FILE_PATH = "data.json";
	private static final String URL = "http://localhost:8081/upload";

	public static void main(String[] args) {
		SpringApplication.run(FileSenderApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		File file = new File(FILE_PATH);
		if (!file.exists()) {
			String sampleJson = "{\"name\":\"Test\",\"value\":123}";
			Files.write(file.toPath(), sampleJson.getBytes());
		}

		logger.info("Reading and compressing JSON file: {}", FILE_PATH);
		byte[] gzippedData = compressFile(file);
		long startTime = System.currentTimeMillis();
		logger.info("Sending compressed JSON file to Microservice B");
		String response = sendFile(gzippedData);
		long endTime = System.currentTimeMillis();
		logger.info("Response from Microservice B: {}", response);
		logger.info("Total time taken: {} ms", (endTime - startTime));
	}

	private byte[] compressFile(File file) throws IOException {
		try (FileInputStream fis = new FileInputStream(file);
			 ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 GZIPOutputStream gos = new GZIPOutputStream(baos)) {

			byte[] buffer = new byte[1024];
			int len;
			while ((len = fis.read(buffer)) != -1) {
				gos.write(buffer, 0, len);
			}
			gos.finish();
			logger.info("JSON file compression completed successfully");
			return baos.toByteArray();
		}
	}

	private String sendFile(byte[] gzippedData) {
		RestTemplate restTemplate = new RestTemplate();
		HttpHeaders headers = new HttpHeaders();
		headers.add("Content-Type", "application/gzip");
		HttpEntity<byte[]> requestEntity = new HttpEntity<>(gzippedData, headers);

		try {
			ResponseEntity<String> response = restTemplate.postForEntity(URL, requestEntity, String.class);
			return response.getBody();
		} catch (Exception e) {
			logger.error("Error sending JSON file to Microservice B", e);
			return "ERROR";
		}
	}
}