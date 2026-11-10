-- 1. Таблица для сырых данных из Kafka (JSONEachRow)
CREATE TABLE IF NOT EXISTS raw_telemetry (
    event_id String,
    equipment_id String,
    factory_id String,
    sensor_id Nullable(String),
    sensor_type String,
    unit String,
    value String,
    status String,
    event_datetime String,
    gateway_id String,
    gateway_protocol String,
    gateway_firmware String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'telemetry.events',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2. Целевая таблица (MergeTree) для хранения сырых данных
CREATE TABLE IF NOT EXISTS raw_telemetry_storage (
    event_id String,
    equipment_id String,
    factory_id String,
    sensor_id Nullable(String),
    sensor_type String,
    unit String,
    value String,
    status String,
    event_datetime String,
    gateway_id String,
    gateway_protocol String,
    gateway_firmware String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_datetime, equipment_id);

-- 3. Materialized View: перекладывает данные из Kafka в MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS raw_telemetry_mv TO raw_telemetry_storage AS
SELECT * FROM raw_telemetry;

-- 4. Таблица карантина для «грязных» данных
CREATE TABLE IF NOT EXISTS quarantine_events (
    event_id String,
    reason String,
    raw_payload String,
    quarantined_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (quarantined_at, event_id);