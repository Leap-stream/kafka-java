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
