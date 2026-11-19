"""
Скрипт выгрузки витрины equipment_health_mart в MinIO (S3).
Запускается из Airflow (внутри контейнера), но может быть запущен и локально.
"""
import datetime
import io
import os
from pathlib import Path

import boto3
import clickhouse_connect
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv

# Загружаем .env из корня проекта (на уровень выше папки scripts/)
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
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


def upload():
    # Секреты — обязательны, дефолтов нет
    ch_user = get_required_env("CLICKHOUSE_USER")
    ch_pass = get_required_env("CLICKHOUSE_PASSWORD")
    minio_access = get_required_env("MINIO_ACCESS_KEY")
    minio_secret = get_required_env("MINIO_SECRET_KEY")

    # Хосты: по умолчанию Docker-имена, локально переопределяются через env
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

    print(f"Чтение витрины из ClickHouse ({ch_host}:8123)...")
    client = clickhouse_connect.get_client(
        host=ch_host,
        port=8123,
        username=ch_user,
        password=ch_pass,
    )

    df = client.query_df("SELECT * FROM equipment_health_mart")
    print(f"Прочитано {len(df)} строк.")

    # Конвертируем в Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine="pyarrow", index=False)
    parquet_buffer.seek(0)

    print(f"Выгрузка в MinIO ({minio_endpoint})...")
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access,
        aws_secret_access_key=minio_secret,
        config=Config(signature_version="s3v4"),
    )

    today_str = datetime.datetime.now().strftime("%Y_%m_%d")
    filename = f"equipment_health_{today_str}.parquet"

    s3.put_object(
        Bucket="analytics-results",
        Key=filename,
        Body=parquet_buffer.getvalue(),
    )
    print(f"Файл {filename} успешно загружен в MinIO!")


if __name__ == "__main__":
    upload()