package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerApp {

    public static void main(String[] args) throws Exception {

        // Only use WSL CSV path
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

        // Store last known state of each symbol
        Map<String, String> lastState = new HashMap<>();

        while (true) {
            boolean hasChange = false;

            try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
                String line;
                boolean isHeader = true;

                while ((line = br.readLine()) != null) {
                    if (isHeader) {
                        isHeader = false; // skip header
                        continue;
                    }
                    String[] cols = line.split("\t"); // tab-separated CSV
                    String key = cols[1]; // symbol
                    String value = line;   // full row

                    // Check for change
                    if (!value.equals(lastState.get(key))) {
                        lastState.put(key, value); // update state
                        RecordMetadata metadata = producer.send(new ProducerRecord<>("nse", key, value)).get();
                        System.out.println("✅ Sent: " + key + " -> " + value + " to partition " + metadata.partition());
                        hasChange = true;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            if (!hasChange) {
                System.out.println("⏳ No change — waiting for next capture...");
            }

            TimeUnit.SECONDS.sleep(60);
        }

        // producer.close(); // never reached
    }
}
