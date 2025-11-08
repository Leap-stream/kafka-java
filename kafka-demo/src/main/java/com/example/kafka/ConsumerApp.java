package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "test";
        String groupId = "test-group";

        // Supabase credentials (replace with your actual values)
        String SUPABASE_URL = "https://qwlwpjojkhrjitmooqkr.supabase.co";
        String SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF3bHdwam9qa2hyaml0bW9vcWtyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI1MjIzOTAsImV4cCI6MjA3ODA5ODM5MH0.V0AiE0C2uVr-2NwQe6xgpE7sTHJW2QS0Njd_yZYJiyc";
        String TABLE_NAME = "kafka_messages"; // You must have created this table in Supabase

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consuming messages from topic: " + topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record key=%s value=%s partition=%d offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());

                    // Push to Supabase
                    sendToSupabase(SUPABASE_URL, SUPABASE_API_KEY, TABLE_NAME, record.value());
                }
            }
        }
    }

    private static void sendToSupabase(String baseUrl, String apiKey, String table, String message) {
        try {
            URL url = new URL(baseUrl + "/rest/v1/" + table);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("apikey", apiKey);
            conn.setRequestProperty("Authorization", "Bearer " + apiKey);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            // Build JSON payload (adjust field name to your table)
            String json = String.format("{\"message\": \"%s\"}", message);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes());
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 201) {
                System.out.println("✅ Message inserted into Supabase: " + message);
            } else {
                System.out.println("⚠️ Failed to insert message. HTTP code: " + responseCode);
            }

            conn.disconnect();
        } catch (Exception e) {
            System.err.println("❌ Error sending to Supabase: " + e.getMessage());
        }
    }
}
