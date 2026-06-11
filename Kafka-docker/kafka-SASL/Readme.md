Test 1 - Verify Authentication

Inside the Kafka container:

docker exec -it kafka bash

Run:

kafka-topics \
--bootstrap-server localhost:9092 \
--command-config /etc/kafka/client.properties \
--list

If authentication works, you should not see:

Unexpected Kafka request of type METADATA during SASL handshake


Test 2 - Create Topic
kafka-topics \
--bootstrap-server localhost:9092 \
--command-config /etc/kafka/client.properties \
--create \
--topic test-topic \
--partitions 1 \
--replication-factor 1

kafka-console-producer --bootstrap-server localhost:9092 --producer.config /etc/kafka/client.properties --topic logs-topic 
