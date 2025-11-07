package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "test";
        String groupId = "test-group";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest"); // Start from beginning

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consuming messages from topic: " + topic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record key=%s value=%s partition=%d offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            }
        }
    }
}

