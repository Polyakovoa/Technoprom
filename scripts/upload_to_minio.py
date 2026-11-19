import datetime
import io
import pandas as pd
import clickhouse_connect
import boto3
from botocore.client import Config

def upload():
    print("Чтение витрины из ClickHouse...")
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='changeme')
    
    # Читаем данные в Pandas DataFrame
    df = client.query_df('SELECT * FROM equipment_health_mart')
    print(f"Прочитано {len(df)} строк.")

    # Конвертируем в Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    parquet_buffer.seek(0)

    print("Выгрузка в MinIO...")
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4')
    )
    
    today_str = datetime.datetime.now().strftime("%Y_%m_%d")
    filename = f"equipment_health_{today_str}.parquet"
    
    s3.put_object(Bucket='analytics-results', Key=filename, Body=parquet_buffer.getvalue())
    print(f"✅ Файл {filename} успешно загружен в MinIO!")

if __name__ == "__main__":
    upload()