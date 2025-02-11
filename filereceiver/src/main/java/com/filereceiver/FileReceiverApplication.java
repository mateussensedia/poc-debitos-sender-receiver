package com.filereceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

@SpringBootApplication
@RestController
public class FileReceiverApplication {

	private static final Logger logger = LoggerFactory.getLogger(FileReceiverApplication.class);
	private static final String FILE_PATH = "received.txt";

	public static void main(String[] args) {
		SpringApplication.run(FileReceiverApplication.class, args);
	}

	@PostMapping("/upload")
	public ResponseEntity<String> receiveFile(@RequestBody byte[] gzippedData) {
		logger.info("Received compressed file, starting decompression");
		try {
			byte[] decompressedData = decompressGzip(gzippedData);
			saveFile(decompressedData);
			logger.info("File saved successfully");
			return ResponseEntity.ok("OK");
		} catch (IOException e) {
			logger.error("Error processing file", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing file");
		}
	}

	private byte[] decompressGzip(byte[] gzippedData) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(gzippedData);
			 GZIPInputStream gis = new GZIPInputStream(bais);
			 ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

			byte[] buffer = new byte[1024];
			int len;
			while ((len = gis.read(buffer)) != -1) {
				baos.write(buffer, 0, len);
			}
			logger.info("File decompression completed");
			return baos.toByteArray();
		}
	}

	private void saveFile(byte[] fileData) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(FILE_PATH)) {
			fos.write(fileData);
		}
	}
}
