This is tailored to:

KRaft mode

SCRAM-SHA-512 enabled

Controller PLAINTEXT (stable bootstrap pattern)

confluentinc/cp-kafka:7.6.1

Docker Compose

Using
Confluent cp-kafka:7.6.1

📄 README.md
Kafka KRaft + SCRAM-SHA-512 Setup (Single Node)

This document describes how to set up a single-node Apache Kafka cluster in KRaft mode with SCRAM-SHA-512 authentication using Docker.

Kafka Image Used:
confluentinc/cp-kafka:7.6.1

📌 Architecture
Component	Security
Controller Listener (9093)	PLAINTEXT
Client/Broker Listener (9092)	SASL_PLAINTEXT (SCRAM-SHA-512)

Why?

Controller remains PLAINTEXT for stable bootstrap

SCRAM enabled only for client and broker communication

Avoids authentication loop during startup

🧱 Step 1 — Generate Cluster ID

KRaft mode requires a CLUSTER_ID.

Generate it once:

docker run --rm confluentinc/cp-kafka:7.6.1 kafka-storage random-uuid

Example output:

MkU3OEVBNTcwNTJENDM2Qg

Save this value.

🐳 Step 2 — Initial Docker Compose (Bootstrap Mode)

Create docker-compose.yml

version: '3.8'

services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka
    ports:
      - "9092:9092"

    volumes:
      - ./kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf

    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      CLUSTER_ID: "PASTE_YOUR_CLUSTER_ID"

      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER

      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092

      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_SUPER_USERS: User:admin;User:broker

      KAFKA_OPTS: -Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

Start Kafka:

docker compose up -d
docker logs -f kafka

Ensure server starts successfully.

👤 Step 3 — Create SCRAM Users

Enter container:

docker exec -it kafka bash

Create broker user:

kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=broker-secret]' \
  --entity-type users \
  --entity-name broker

Create admin user:

kafka-configs --bootstrap-server localhost:9092 \
  --alter \
  --add-config 'SCRAM-SHA-512=[password=admin-secret]' \
  --entity-type users \
  --entity-name admin

Exit container.

🔐 Step 4 — Enable SCRAM

Modify docker-compose.yml:

Change listeners:

KAFKA_LISTENERS: SASL_PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: SASL_PLAINTEXT://localhost:9092

KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: SASL_PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: SASL_PLAINTEXT

KAFKA_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512
KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL: SCRAM-SHA-512
📄 Step 5 — JAAS File

Create kafka_server_jaas.conf

KafkaServer {
  org.apache.kafka.common.security.scram.ScramLoginModule required
  username="broker"
  password="broker-secret";
};
🔁 Step 6 — Restart Kafka
docker compose down
docker compose up -d
docker logs -f kafka

Kafka should start successfully.

✅ Verification

Test authentication:

Create client.properties:

security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
  username="admin" \
  password="admin-secret";

Test:

kafka-topics --bootstrap-server localhost:9092 \
  --command-config client.properties \
  --list

If it lists topics → SCRAM works.

🧠 Important Notes

Controller runs PLAINTEXT for bootstrap stability

Broker authenticates as broker

Admin user is used for client operations

Do NOT change CLUSTER_ID after first initialization

If changing CLUSTER_ID, delete Kafka data directory

🎯 Achieved Checkpoint

✔ KRaft mode running
✔ SCRAM-SHA-512 enabled
✔ Broker ↔ broker authentication working
✔ Client authentication working
✔ Super users configured
✔ Production-style bootstrap sequence

If you want next level:

Add ACL enforcement

Automate SCRAM user creation via Jenkins

Integrate Vault for password injection

Enable SSL + SCRAM

You’ve now crossed the “Kafka Security Beginner” phase 🚀
