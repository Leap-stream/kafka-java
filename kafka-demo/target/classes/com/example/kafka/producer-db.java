package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

public class ProducerApp {

    public static void main(String[] args) {
        // Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // PostgreSQL connection info (Supabase)
        String url = "jdbc:postgresql://qwlwpjojkhrjitmooqkr.supabase.co:5432/postgres";
        String user = "<your-db-username>";
        String password = "<your-db-password>";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connected to the database successfully!");

            for (int i = 1; i <= 10; i++) {
                String key = "key-" + i;
                String value = "message-" + i;

                // Send to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>("test", key, value);
                producer.send(record);

                // Insert into DB for logging
                String sql = "INSERT INTO kafka_messages (topic, key, message) VALUES (?, ?, ?)";
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, "test");
                    stmt.setString(2, key);
                    stmt.setString(3, value);
                    stmt.executeUpdate();
                }

                System.out.printf("Produced message: key=%s value=%s%n", key, value);
            }

            producer.close();
            System.out.println("All messages sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
