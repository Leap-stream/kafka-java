package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerApp {

    private static final String CSV_FILE = "/home/arbaz/kafka-dev/confluent-kafka/new/nse/nifty50_prices.csv";

    public static void main(String[] args) throws Exception {

        KafkaProducer<String, String> producer = new KafkaProducer<>(getProps());

        long filePointer = 0; // Tracks where we left off in the file

        System.out.println("📌 Monitoring CSV: " + CSV_FILE);

        while (true) {
            try (RandomAccessFile raf = new RandomAccessFile(CSV_FILE, "r")) {

                // Move to last read position
                raf.seek(filePointer);

                String line;
                boolean hasNewData = false;

                while ((line = raf.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    // Skip header if at beginning
                    if (filePointer == 0 && line.toLowerCase().contains("timestamp")) continue;

                    String[] cols;
                    if (line.contains("\t")) {
                        cols = line.split("\t");
                    } else {
                        cols = line.split(",");
                    }

                    if (cols.length < 2) continue;

                    String key = cols[1];   // symbol
                    String value = line;     // full row

                    producer.send(new ProducerRecord<>("nse", key, value)).get();
                    System.out.println("✅ Sent: " + key + " -> " + value);
                    hasNewData = true;
                }

                // Update file pointer
                filePointer = raf.getFilePointer();

                if (!hasNewData) {
                    System.out.println("⏳ No new messages — waiting for next capture...");
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            TimeUnit.SECONDS.sleep(60);
        }
    }

    private static Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", Integer.MAX_VALUE);
        props.put("enable.idempotence", "true");
        props.put("max.in.flight.requests.per.connection", "1");

        return props;
    }
}
