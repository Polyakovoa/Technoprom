"""
Загрузчик справочников из MongoDB в ClickHouse RAW.
Запуск: python load_mongo_to_clickhouse.py
"""
import json
import clickhouse_connect
from pymongo import MongoClient

MONGO_URI = "mongodb://admin:password@localhost:27017/"
mongo = MongoClient(MONGO_URI)["technoprom"]

ch = clickhouse_connect.get_client(
    host="localhost", 
    port=8123, 
    username="default", 
    password="changeme"
)


def flatten_manager(doc):
    mgr = doc.get("manager") or {}
    return {
        **{k: v for k, v in doc.items() if k != "manager"},
        "manager_employee_id": mgr.get("employee_id", ""),
        "manager_name": mgr.get("name", ""),
        "manager_email": mgr.get("email", ""),
        "manager_phone": mgr.get("phone", ""),
    }


def flatten_ticket(doc):
    eng = doc.get("engineer") or {}
    return {
        **{k: v for k, v in doc.items() if k not in ("engineer", "parts_used")},
        "engineer_id": eng.get("engineer_id", ""),
        "engineer_name": eng.get("name", ""),
        "engineer_qualification": eng.get("qualification", ""),
        "parts_used_json": json.dumps(doc.get("parts_used", []), ensure_ascii=False),
    }


def load(collection, table, transformer=None):
    docs = list(mongo[collection].find({}, {"_id": 0}))
    if not docs:
        print(f"  {collection}: пусто, пропускаем")
        return
    rows = [transformer(d) if transformer else d for d in docs]
    columns = list(rows[0].keys())
    data = [[r.get(c) for c in columns] for r in rows]
    ch.insert(table, data, column_names=columns)
    print(f"  {collection} -> {table}: {len(rows)} строк")


print("Загрузка справочников из MongoDB в ClickHouse RAW...")
load("factories",      "raw_factories",      flatten_manager)
load("equipment",      "raw_equipment")
load("sensors",        "raw_sensors")
load("engineers",      "raw_engineers")
load("spare_parts",    "raw_spare_parts")
load("failures",       "raw_failures")
load("repair_tickets", "raw_repair_tickets", flatten_ticket)
print("Готово.")