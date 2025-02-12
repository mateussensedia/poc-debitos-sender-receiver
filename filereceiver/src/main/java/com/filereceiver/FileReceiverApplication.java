package com.filereceiver;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.io.PrintWriter;
import java.util.List;
import java.util.zip.GZIPInputStream;

@SpringBootApplication
@RestController
public class FileReceiverApplication {

    private static final Logger logger = LoggerFactory.getLogger(FileReceiverApplication.class);
    private static final String FILE_PATH = "received.txt";
    private static final String JSON_FILE_PATH = "received.json";

    public static void main(String[] args) {
        SpringApplication.run(FileReceiverApplication.class, args);
    }

    @PostMapping("/upload")
    public ResponseEntity<String> receiveFile(@RequestBody byte[] gzippedData) {
        logger.info("Received compressed file, starting decompression");
        try {
            byte[] decompressedData = decompressGzip(gzippedData);
            saveFile(decompressedData, FILE_PATH);
            logger.info("File saved successfully");
            return ResponseEntity.ok("OK");
        } catch (IOException e) {
            logger.error("Error processing file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing file");
        }
    }

    @PostMapping("/process")
    public ResponseEntity<String> processLargeData(@RequestBody List<Object> data) {
        logger.info("Received batch of {} objects for processing", data.size());
        long startTime = System.currentTimeMillis();
        try {
            saveJsonFile(data, JSON_FILE_PATH);
            long endTime = System.currentTimeMillis();
            logger.info("Data successfully saved to file. Processing time: {} ms", (endTime - startTime));
            return ResponseEntity.ok("Data saved successfully in " + (endTime - startTime) + " ms");
        } catch (IOException e) {
            logger.error("Error saving data to file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error saving data");
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

    private void saveFile(byte[] fileData, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(fileData);
        }
    }

    private void saveJsonFile(List<Object> data, String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(filePath))) {
            objectMapper.writeValue(writer, data);
        }
    }
}