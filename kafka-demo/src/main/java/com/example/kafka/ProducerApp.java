package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerApp {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "test";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all"); // Ensure message is committed to broker

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String key = "key-" + i;
                String value = "Message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
                Future<RecordMetadata> future = producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending message: " + exception.getMessage());
                    } else {
                        System.out.printf("Sent record key=%s value=%s partition=%d offset=%d%n",
                                key, value, metadata.partition(), metadata.offset());
                    }
                });
            }
        }

        System.out.println("All messages produced.");
    }
}

