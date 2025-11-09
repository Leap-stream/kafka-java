package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.Properties;

import com.google.gson.JsonObject;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class KafkaConsumerToSupabase {

    private static final String BASE_URL = "https://qwlwpjojkhrjitmooqkr.supabase.co";
    private static final String SUPABASE_API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InF3bHdwam9qa2hyaml0bW9vcWtyIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI1MjIzOTAsImV4cCI6MjA3ODA5ODM5MH0.V0AiE0C2uVr-2NwQe6xgpE7sTHJW2QS0Njd_yZYJiyc";
    private static final String TABLE_NAME = "kafka_messages";

    public static void main(String[] args) throws Exception {
        // Kafka consumer config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "supabase-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("nse"));

        CloseableHttpClient httpClient = HttpClients.createDefault();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                String line = record.value();
                String[] cols = line.split(",");

                // Skip malformed lines
                if (cols.length < 6) {
                    System.out.println("⚠ Skipping malformed line: " + line);
                    continue;
                }

                try {
                    JsonObject payload = new JsonObject();
                    payload.addProperty("timestamp", cols[0].trim());
                    payload.addProperty("symbol", cols[1].trim());
                    payload.addProperty("last_price", Double.parseDouble(cols[2].trim()));
                    payload.addProperty("change", Double.parseDouble(cols[3].trim()));
                    payload.addProperty("pchange", Double.parseDouble(cols[4].trim()));
                    payload.addProperty("volume", Long.parseLong(cols[5].trim()));

                    HttpPost request = new HttpPost(BASE_URL + "/rest/v1/" + TABLE_NAME);
                    request.addHeader("apikey", SUPABASE_API_KEY);
                    request.addHeader("Authorization", "Bearer " + SUPABASE_API_KEY);
                    request.addHeader("Content-Type", "application/json");
                    request.setEntity(new StringEntity(payload.toString()));

                    httpClient.execute(request).close();
                    System.out.println("✅ Pushed: " + cols[1].trim() + " at " + cols[0].trim());
                } catch (Exception e) {
                    System.out.println("❌ Failed to push: " + line);
                    e.printStackTrace();
                }
            }
        }
    }
}

