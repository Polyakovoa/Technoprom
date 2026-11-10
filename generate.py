"""
ТехноПром — Генератор данных.
Пишет справочники в MongoDB, а телеметрию стримит в Kafka.
Запуск: python generate.py
"""
import json
import random
import time
from datetime import datetime, timedelta, timezone
from faker import Faker
from pymongo import MongoClient
from kafka import KafkaProducer

fake = Faker("ru_RU")
random.seed(42)
Faker.seed(42)

# --- Конфигурация подключений ---
MONGO_URI = "mongodb://admin:password@localhost:27017/"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetry.events"

# --- Подключение к сервисам ---
print("Подключение к MongoDB...")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["technoprom"]

print("Подключение к Kafka...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    linger_ms=10, # Оптимизация: собираем сообщения в батчи
    batch_size=16384
)

# --- Вспомогательные функции ---
def iso_z(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def make_phone():
    return f"+7 {random.randint(900, 999)} {random.randint(100, 999)}-{random.randint(10, 99)}-{random.randint(10, 99)}"

# --- Константы ---
NOW = datetime(2026, 9, 30, 23, 59, 59, tzinfo=timezone.utc)
N_FACTORIES = 25
N_ENGINEERS = 50
N_PARTS = 100
N_FAILURES = 1600
N_TICKETS = 850
TELEMETRY_DAYS = 30
TELEMETRY_PER_DAY = 4500  # ~135k событий

EQUIPMENT_TYPES = ["CNC/machine", "pump", "compressor", "conveyor", "refrigeration_unit"]
CRITICALITY = ["low", "medium", "high", "critical"]
SENSOR_PROFILES = {
    "temperature": {"unit": "celsius", "min": 10.0, "max": 85.0},
    "vibration": {"unit": "mm/s", "min": 0.5, "max": 12.0},
    "pressure": {"unit": "bar", "min": 1.0, "max": 16.0},
    "power_consumption": {"unit": "kW", "min": 0.5, "max": 45.0},
    "rpm": {"unit": "rpm", "min": 500.0, "max": 3500.0},
}
FAILURE_TYPES = ["overheating", "vibration_anomaly", "pressure_drop", "power_surge", "bearing_wear", "leak"]
SEVERITIES = ["warning", "critical"]
QUALIFICATIONS = ["junior", "middle", "senior"]
PART_CATEGORIES = ["bearing", "seal", "filter", "belt", "motor", "valve", "sensor", "cable"]
MANUFACTURERS = ["ПромМаш", "ТехноСтан", "СтанкоПром", "УралТехМаш", "РусПромОборудование"]

REGIONS_CITIES = [
    ("Республика Татарстан", "Казань"), ("Московская область", "Подольск"),
    ("Свердловская область", "Екатеринбург"), ("Ленинградская область", "Гатчина"),
    ("Нижегородская область", "Дзержинск"), ("Челябинская область", "Магнитогорск"),
    ("Самарская область", "Тольятти"), ("Ростовская область", "Таганрог"),
    ("Новосибирская область", "Новосибирск"), ("Краснодарский край", "Краснодар"),
    ("Пермский край", "Пермь"), ("Воронежская область", "Воронеж"),
    ("Волгоградская область", "Волгоград"), ("Омская область", "Омск"),
    ("Иркутская область", "Ангарск"), ("Тюменская область", "Тюмень"),
    ("Республика Башкортостан", "Стерлитамак"), ("Красноярский край", "Красноярск"),
    ("Приморский край", "Артём"), ("Саратовская область", "Балаково"),
    ("Белгородская область", "Старый Оскол"), ("Тульская область", "Новомосковск"),
    ("Ярославская область", "Рыбинск"), ("Кемеровская область", "Новокузнецк"),
    ("Алтайский край", "Бийск"),
]

# ==========================================
# 1. ГЕНЕРАЦИЯ СПРАВОЧНИКОВ -> MONGODB
# ==========================================

def load_to_mongo(collection_name, data):
    db[collection_name].drop() # Очищаем перед новым запуском
    if data:
        db[collection_name].insert_many(data)
    print(f"  -> {collection_name}: {len(data)} док-в")

print("[1/7] Factories...")
factories = []
for i, (region, city) in enumerate(REGIONS_CITIES, start=1):
    fid = f"factory-{i:03d}"
    factories.append({
        "factory_id": fid,
        "name": f"ТехноПром — {city}",
        "region": region, "city": city,
        "address": f"ул. Промышленная, {random.randint(1, 50)}",
        "timezone": "Europe/Moscow",
        "opened_at": fake.date_between(start_date="-25y", end_date="-2y").isoformat(),
        "criticality": random.choice(CRITICALITY),
        "manager": {
            "employee_id": f"mgr-{i:03d}", "name": fake.name(),
            "email": fake.company_email(), "phone": make_phone(),
        },
    })
load_to_mongo("factories", factories)

print("[2/7] Equipment...")
equipment = []
eq_counter = 0
for factory in factories:
    for _ in range(random.randint(12, 14)):
        eq_counter += 1
        etype = random.choice(EQUIPMENT_TYPES)
        display_type = etype
        if random.random() < 0.02: # Dirty: разный регистр
            display_type = random.choice([etype.upper(), etype.capitalize(), etype.lower()])
        
        equipment.append({
            "equipment_id": f"eq-{eq_counter:05d}",
            "factory_id": factory["factory_id"],
            "name": f"{etype} {fake.bothify(text='??-###').upper()}",
            "equipment_type": display_type,
            "manufacturer": random.choice(MANUFACTURERS),
            "model": fake.bothify(text="??-###").upper(),
            "serial_number": f"SN-{fake.bothify(text='??-#####').upper()}",
            "commissioned_at": fake.date_between(start_date="-15y", end_date="-1y").isoformat(),
            "status": random.choices(["active", "maintenance", "inactive"], weights=[0.92, 0.06, 0.02])[0],
            "criticality": random.choice(CRITICALITY),
            "maintenance_interval_days": random.choice([30, 60, 90, 180, 365]),
            "last_maintenance_at": fake.date_between(start_date="-1y", end_date="today").isoformat(),
        })
load_to_mongo("equipment", equipment)

print("[3/7] Sensors...")
sensors = []
sensor_counter = 0
for eq in equipment:
    for stype in random.sample(list(SENSOR_PROFILES.keys()), random.randint(4, 5)):
        sensor_counter += 1
        profile = SENSOR_PROFILES[stype]
        sensors.append({
            "sensor_id": f"sensor-{sensor_counter:06d}",
            "equipment_id": eq["equipment_id"],
            "sensor_type": stype, "unit": profile["unit"],
            "min_normal": profile["min"], "max_normal": profile["max"],
            "installed_at": fake.date_between(start_date="-5y", end_date="today").isoformat(),
            "status": random.choices(["active", "inactive", "faulty"], weights=[0.94, 0.04, 0.02])[0],
            "sampling_interval_sec": random.choice([30, 60, 120, 300]),
        })
load_to_mongo("sensors", sensors)

print("[4/7] Engineers & Spare Parts...")
engineers = []
for i in range(1, N_ENGINEERS + 1):
    engineers.append({
        "engineer_id": f"eng-{i:03d}",
        "first_name": fake.first_name_male(), "last_name": fake.last_name_male(),
        "email": fake.company_email(), "phone": make_phone(),
        "qualification": random.choice(QUALIFICATIONS),
        "specializations": random.sample(EQUIPMENT_TYPES, k=random.randint(1, 3)),
        "home_factory_id": random.choice(factories)["factory_id"],
        "active": random.choices([True, False], weights=[0.9, 0.1])[0],
    })
load_to_mongo("engineers", engineers)

parts = []
for i in range(1, N_PARTS + 1):
    parts.append({
        "part_id": f"part-{i:04d}",
        "name": f"{random.choice(['Подшипник','Сальник','Фильтр','Ремень','Двигатель','Клапан','Кабель','Датчик'])} промышленный",
        "category": random.choice(PART_CATEGORIES), "manufacturer": random.choice(MANUFACTURERS),
        "price": round(random.uniform(500, 50000), 2), "stock_quantity": random.randint(0, 200),
        "warehouse_factory_id": random.choice(factories)["factory_id"],
    })
load_to_mongo("spare_parts", parts)

print("[5/7] Failures & Tickets...")
failures = []
for i in range(1, N_FAILURES + 1):
    eq = random.choice(equipment)
    detected = NOW - timedelta(days=random.randint(0, 180), hours=random.randint(0, 23))
    f_dict = {
        "failure_id": f"fail-{i:06d}",
        "equipment_id": eq["equipment_id"], "factory_id": eq["factory_id"],
        "failure_type": random.choice(FAILURE_TYPES), "severity": random.choice(SEVERITIES),
        "detected_at": iso_z(detected), "source": random.choice(["sensor", "manual", "scheduled_check"]),
        "description": fake.sentence(nb_words=6), "acknowledged": random.random() < 0.75,
    }
    if random.random() < 0.003: f_dict["equipment_id"] = "eq-99999" # Dirty
    failures.append(f_dict)
load_to_mongo("failures", failures)

tickets = []
for i in range(1, N_TICKETS + 1):
    eq = random.choice(equipment)
    eng = random.choice(engineers)
    failure = random.choice(failures) if random.random() < 0.6 else None
    created = NOW - timedelta(days=random.randint(0, 180), hours=random.randint(0, 23))
    status = random.choices(["open", "in_progress", "closed", "cancelled"], weights=[0.10, 0.15, 0.70, 0.05])[0]
    started = finished = None
    if status in ("in_progress", "closed"): started = created + timedelta(minutes=random.randint(5, 240))
    if status == "closed":
        finished = started + timedelta(minutes=random.randint(30, 480))
        if random.random() < 0.02: finished = created - timedelta(minutes=random.randint(10, 120)) # Dirty
    
    parts_used = []
    if status == "closed" and random.random() < 0.7:
        for _ in range(random.randint(1, 3)):
            p = random.choice(parts)
            parts_used.append({"part_id": p["part_id"], "quantity": random.randint(1, 5), "price": p["price"]})
            
    downtime = int((finished - started).total_seconds() / 60) if (started and finished and finished > started) else None

    tickets.append({
        "ticket_id": f"repair-{i:06d}", "equipment_id": eq["equipment_id"], "factory_id": eq["factory_id"],
        "created_at": iso_z(created), "priority": random.choice(["low", "medium", "high", "critical"]),
        "failure_type": failure["failure_type"] if failure else random.choice(FAILURE_TYPES),
        "description": fake.sentence(nb_words=8),
        "engineer": {"engineer_id": eng["engineer_id"], "name": f"{eng['first_name']} {eng['last_name']}", "qualification": eng["qualification"]},
        "status": status, "started_at": iso_z(started) if started else None,
        "finished_at": iso_z(finished) if finished else None, "parts_used": parts_used, "downtime_minutes": downtime,
    })
load_to_mongo("repair_tickets", tickets)

# ==========================================
# 2. ГЕНЕРАЦИЯ ТЕЛЕМЕТРИИ -> KAFKA
# ==========================================
print(f"[6/7] Генерация телеметрии в Kafka (топик: {KAFKA_TOPIC})...")

eq_by_id = {e["equipment_id"]: e for e in equipment}
gap_sensors = random.sample([s["sensor_id"] for s in sensors], k=5)
gap_windows = {sid: (NOW - timedelta(days=random.randint(1, 25), hours=random.randint(0, 20)), 
                     NOW - timedelta(days=random.randint(1, 25)) + timedelta(hours=random.randint(4, 8))) for sid in gap_sensors}

def generate_measurement(sensor):
    profile = SENSOR_PROFILES[sensor["sensor_type"]]
    lo, hi = profile["min"], profile["max"]
    r = random.random()
    if r < 0.95: return random.uniform(lo, hi), "normal"
    if r < 0.99: return (random.uniform(hi, hi * 1.15) if random.random() < 0.5 else random.uniform(lo * 0.85, lo)), "warning"
    return (random.uniform(hi * 1.2, hi * 1.5) if random.random() < 0.5 else random.uniform(lo * 0.5, lo * 0.8)), "critical"

event_counter = 0
messages_sent = 0

print("  -> Отправка данных...")
for day_idx in range(TELEMETRY_DAYS):
    day_start = NOW.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=TELEMETRY_DAYS - 1 - day_idx)
    
    for _ in range(TELEMETRY_PER_DAY):
        sensor = random.choice(sensors)
        eq = eq_by_id[sensor["equipment_id"]]
        ts = day_start + timedelta(seconds=random.randint(0, 86399))

        if sensor["sensor_id"] in gap_windows:
            gs, ge = gap_windows[sensor["sensor_id"]]
            if gs <= ts <= ge: continue

        value, status = generate_measurement(sensor)
        rv = random.random()
        
        if rv < 0.01: value_str = "N/A" if random.random() < 0.5 else f"{value:.1f}".replace(".", ",") # Dirty
        elif rv < 0.02: value_str = f"{value * 200:.1f}" # Dirty outlier
        else: value_str = f"{value:.2f}"

        event_counter += 1
        
        # Формируем "плоский" JSON для ClickHouse
        event = {
            "event_id": f"evt-{event_counter:08d}",
            "equipment_id": eq["equipment_id"],
            "factory_id": eq["factory_id"],
            "sensor_id": sensor["sensor_id"],
            "sensor_type": sensor["sensor_type"],
            "unit": sensor["unit"],
            "value": value_str,
            "status": status,
            "event_datetime": iso_z(ts),
            "gateway_id": f"gw-{random.randint(1, 50):04d}",
            "gateway_protocol": random.choice(["mqtt", "opcua", "modbus"]),
            "gateway_firmware": f"{random.randint(1,4)}.{random.randint(0,9)}.{random.randint(0,9)}",
        }

        if random.random() < 0.02: event["sensor_id"] = None # Dirty
        if random.random() < 0.005: event["equipment_id"] = "eq-99999" # Dirty

        # Отправка в Kafka
        producer.send(KAFKA_TOPIC, event)
        messages_sent += 1

        # Небольшая задержка, чтобы Grafana видела поток в реальном времени
        if messages_sent % 100 == 0:
            time.sleep(0.05) # 50 миллисекунд каждые 100 сообщений

        if messages_sent % 10000 == 0:
            print(f"     Отправлено: {messages_sent}...")

print("  -> Ожидание завершения отправки (flush)...")
producer.flush()
print("[7/7] Готово!")
print("===================================")
print("Все данные успешно сгенерированы и отправлены в кластер!")