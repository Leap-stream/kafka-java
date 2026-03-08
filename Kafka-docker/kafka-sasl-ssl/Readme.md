Kafka SASL_SSL Setup (Docker + KRaft)

This guide explains how to set up a secure Apache Kafka cluster with SASL + SSL (SASL_SSL) from scratch using Docker.

Security layers used:

Layer	Purpose
TLS / SSL	Encrypt network traffic
SASL	Authenticate users

Connection flow:

Client
   │
   │ 1. TLS Handshake (certificate verification)
   │
   │ 2. SASL Authentication (username/password)
   │
Kafka Broker
1. Project Structure

Create the following structure:

kafka-sasl-ssl
│
├── docker-compose.yml
├── kafka_server_jaas.conf
├── client.properties
└── certs/
2. Create Project Folder
mkdir kafka-sasl-ssl
cd kafka-sasl-ssl
3. Generate SSL Certificates

Create a certificates folder.

mkdir certs
cd certs
3.1 Create Certificate Authority
openssl req -new -x509 \
-keyout ca-key \
-out ca-cert \
-days 365 \
-nodes \
-subj "/CN=kafka-ca"
3.2 Create Kafka Broker Keystore
keytool -keystore kafka.server.keystore.jks \
-alias localhost \
-validity 365 \
-genkey \
-keyalg RSA

Use password:

kafka123
3.3 Create Certificate Request
keytool -keystore kafka.server.keystore.jks \
-alias localhost \
-certreq \
-file cert-file
3.4 Sign Certificate with CA
openssl x509 -req \
-CA ca-cert \
-CAkey ca-key \
-in cert-file \
-out cert-signed \
-days 365 \
-CAcreateserial
3.5 Import CA into Keystore
keytool -keystore kafka.server.keystore.jks \
-alias CARoot \
-import \
-file ca-cert
3.6 Import Signed Certificate
keytool -keystore kafka.server.keystore.jks \
-alias localhost \
-import \
-file cert-signed
3.7 Create Truststore
keytool -keystore kafka.server.truststore.jks \
-alias CARoot \
-import \
-file ca-cert

Password:

kafka123

You should now have:

certs/
 ├── ca-cert
 ├── kafka.server.keystore.jks
 └── kafka.server.truststore.jks
4. Create JAAS Configuration

Create file:

kafka_server_jaas.conf

Contents:

KafkaServer {
 org.apache.kafka.common.security.plain.PlainLoginModule required
 username="admin"
 password="admin-secret"
 user_admin="admin-secret"
 user_appuser="app-secret";
};

Users created:

Username	Password
admin	admin-secret
appuser	app-secret
5. Create Client Configuration

Create:

client.properties

Contents:

security.protocol=SASL_SSL
sasl.mechanism=PLAIN

sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
username="admin" \
password="admin-secret";

ssl.truststore.location=/etc/kafka/secrets/kafka.server.truststore.jks
ssl.truststore.password=kafka123
6. Docker Compose Configuration

Create:

docker-compose.yml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka

    ports:
      - "9092:9092"

    volumes:
      - ./certs:/etc/kafka/secrets
      - ./kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf
      - ./client.properties:/etc/kafka/client.properties

    environment:

      KAFKA_OPTS: "-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf"

      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093

      KAFKA_LISTENERS: SASL_SSL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: SASL_SSL://kafka:9092

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,SASL_SSL:SASL_SSL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: SASL_SSL

      KAFKA_SSL_KEYSTORE_FILENAME: kafka.server.keystore.jks
      KAFKA_SSL_KEYSTORE_PASSWORD: kafka123

      KAFKA_SSL_TRUSTSTORE_FILENAME: kafka.server.truststore.jks
      KAFKA_SSL_TRUSTSTORE_PASSWORD: kafka123

      KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
      KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: PLAIN

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
7. Start Kafka

Run:

docker compose up -d

Check logs:

docker logs kafka

You should see:

Kafka Server started
8. Test SASL_SSL Connection
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:9092 \
--command-config /etc/kafka/client.properties \
--list

Expected output:

__consumer_offsets
9. Create a Topic
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:9092 \
--command-config /etc/kafka/client.properties \
--create \
--topic test-topic \
--partitions 1 \
--replication-factor 1
10. Python Client Example

Example using confluent_kafka Python client.

from confluent_kafka import Producer

conf = {
 "bootstrap.servers": "localhost:9092",
 "security.protocol": "SASL_SSL",
 "sasl.mechanism": "PLAIN",
 "sasl.username": "admin",
 "sasl.password": "admin-secret",
 "ssl.ca.location": "certs/ca-cert"
}

producer = Producer(conf)
producer.produce("test-topic", "hello kafka")
producer.flush()
11. Stop Kafka
docker compose down
Summary

This setup provides:

Kafka running in KRaft mode

TLS encryption

SASL authentication

Dockerized environment

CLI + Python client support

Security mode used:

SASL_SSL
