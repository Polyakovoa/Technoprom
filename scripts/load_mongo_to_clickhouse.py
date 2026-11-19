"""
Загрузчик справочников из MongoDB в ClickHouse RAW.
Запуск: python scripts/load_mongo_to_clickhouse.py
"""
import json
import os
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv
from pymongo import MongoClient

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


def get_required_env(name: str) -> str:
    """Возвращает переменную окружения или падает с понятной ошибкой."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Переменная окружения '{name}' не задана! "
            f"Скопируйте .env.example в .env и заполните значения."
        )
    return value


MONGO_USER = get_required_env("MONGO_USER")
MONGO_PASSWORD = get_required_env("MONGO_PASSWORD")
CH_USER = get_required_env("CLICKHOUSE_USER")
CH_PASS = get_required_env("CLICKHOUSE_PASSWORD")

MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@localhost:27017/"

mongo = MongoClient(MONGO_URI)["technoprom"]
ch = clickhouse_connect.get_client(
    host="localhost",
    port=8123,
    username=CH_USER,
    password=CH_PASS,
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


if __name__ == "__main__":
    print("Загрузка справочников из MongoDB в ClickHouse RAW...")
    load("factories",      "raw_factories",      flatten_manager)
    load("equipment",      "raw_equipment")
    load("sensors",        "raw_sensors")
    load("engineers",      "raw_engineers")
    load("spare_parts",    "raw_spare_parts")
    load("failures",       "raw_failures")
    load("repair_tickets", "raw_repair_tickets", flatten_ticket)
    print("Готово.")