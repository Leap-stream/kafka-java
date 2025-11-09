package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerApp {

    public static void main(String[] args) throws Exception {

        // CSV path inside WSL
        String csvFile = "/home/arbaz/kafka-dev/confluent-kafka/new/nse/nifty50_prices.csv";

        File f = new File(csvFile);
        if (!f.exists()) {
            System.out.println("❌ CSV file not found at: " + csvFile);
            return;
        }

        System.out.println("📌 Reading CSV: " + csvFile);

        // Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Read and push data every 60 seconds
        while (true) {
            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
                String line;
                boolean isHeader = true;

                while ((line = br.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false; // skip header
                        continue;
                    }
                    // Each line is pushed as a message
                    String[] cols = line.split(","); // assuming comma-separated CSV
                    if (cols.length < 2) continue; // skip malformed lines
                    String key = cols[1]; // symbol as key
                    String value = line;   // full row as value
                    RecordMetadata metadata = producer.send(new ProducerRecord<>("nse", key, value)).get();
                    System.out.println("✅ Sent: " + key + " -> " + value + " to partition " + metadata.partition());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("⏳ Waiting 60 seconds before next push...");
            TimeUnit.SECONDS.sleep(60);
        }

        // producer.close(); // never reached in infinite loop
    }
}
