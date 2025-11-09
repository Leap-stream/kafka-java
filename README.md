# kafka-java


Table command:
create table kafka_messages (
    id bigserial primary key,
    timestamp timestamptz,
    symbol text,
    last_price numeric,
    change numeric,
    pchange numeric,
    volume bigint
);

Imp cmds:


./kafka-storage format \
  --config /home/arbaz/kafka-dev/confluent-kafka/confluent-8.1.0/etc/kafka/server.properties \
  --cluster-id wyU9N1UrSmmvti4bsyN-3g \
  --ignore-formatted

# Run the consumer
 mvn clean compile exec:java -Dexec.mainClass="com.example.kafka.KafkaConsumerToSupabase"
 
# Run the producer
mvn exec:java -Dexec.mainClass="com.example.kafka.ProducerApp"


./kafka-server-start /home/arbaz/kafka-dev/confluent-kafka/confluent-8.1.0/etc/kafka/server.properties 
