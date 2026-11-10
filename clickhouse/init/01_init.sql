-- RAW TELEMETRY (из Kafka)
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
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'telemetry.events',
    kafka_group_name = 'clickhouse_consumer_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

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
    -- Технические поля RAW
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'kafka',
    source_file String DEFAULT 'telemetry.events',
    kafka_partition Int32 DEFAULT -1,
    kafka_offset Int64 DEFAULT -1
) ENGINE = MergeTree()
ORDER BY (event_datetime, equipment_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_telemetry_mv TO raw_telemetry_storage AS
SELECT
    event_id, equipment_id, factory_id,
    sensor_id, sensor_type, unit, value, status,
    event_datetime, gateway_id, gateway_protocol, gateway_firmware,
    now() AS ingestion_datetime,
    'kafka' AS source_system,
    _topic AS source_file,
    _partition AS kafka_partition,
    _offset AS kafka_offset
FROM raw_telemetry;

-- RAW FACTORIES (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_factories (
    factory_id String,
    name String,
    region String,
    city String,
    address String,
    timezone String,
    opened_at String,
    criticality String,
    manager_employee_id String,
    manager_name String,
    manager_email String,
    manager_phone String,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'factories'
) ENGINE = MergeTree()
ORDER BY factory_id;

-- RAW EQUIPMENT (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_equipment (
    equipment_id String,
    factory_id String,
    name String,
    equipment_type String,
    manufacturer String,
    model String,
    serial_number String,
    commissioned_at String,
    status String,
    criticality String,
    maintenance_interval_days Int32,
    last_maintenance_at String,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'equipment'
) ENGINE = MergeTree()
ORDER BY equipment_id;

-- RAW SENSORS (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_sensors (
    sensor_id String,
    equipment_id String,
    sensor_type String,
    unit String,
    min_normal Float64,
    max_normal Float64,
    installed_at String,
    status String,
    sampling_interval_sec Int32,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'sensors'
) ENGINE = MergeTree()
ORDER BY sensor_id;

-- RAW ENGINEERS (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_engineers (
    engineer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    qualification String,
    specializations Array(String),
    home_factory_id String,
    active Bool,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'engineers'
) ENGINE = MergeTree()
ORDER BY engineer_id;

-- RAW SPARE PARTS (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_spare_parts (
    part_id String,
    name String,
    category String,
    manufacturer String,
    price Float64,
    stock_quantity Int32,
    warehouse_factory_id String,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'spare_parts'
) ENGINE = MergeTree()
ORDER BY part_id;

-- RAW FAILURES (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_failures (
    failure_id String,
    equipment_id String,
    factory_id String,
    failure_type String,
    severity String,
    detected_at String,
    source String,
    description String,
    acknowledged Bool,
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'failures'
) ENGINE = MergeTree()
ORDER BY failure_id;

-- RAW REPAIR TICKETS (из MongoDB)
CREATE TABLE IF NOT EXISTS raw_repair_tickets (
    ticket_id String,
    equipment_id String,
    factory_id String,
    created_at String,
    priority String,
    failure_type String,
    description String,
    engineer_id String,
    engineer_name String,
    engineer_qualification String,
    status String,
    started_at Nullable(String),
    finished_at Nullable(String),
    parts_used_json String,
    downtime_minutes Nullable(Int32),
    ingestion_datetime DateTime DEFAULT now(),
    source_system String DEFAULT 'mongodb',
    source_file String DEFAULT 'repair_tickets'
) ENGINE = MergeTree()
ORDER BY ticket_id;

-- QUARANTINE (для DQ-проверок в Части IV)
CREATE TABLE IF NOT EXISTS quarantine_events (
    event_id String,
    reason String,
    raw_payload String,
    quarantined_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (quarantined_at, event_id);