from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import clickhouse_connect

# Функции для проверок
def check_sources():
    """Проверяем, что в RAW-слое есть данные для обработки."""
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='changeme')
    res = client.query('SELECT count() FROM raw_telemetry_storage').result_rows[0][0]
    if res == 0:
        raise ValueError("ОШИБКА: Таблица raw_telemetry_storage пуста!")
    print(f"Источник данных доступен! Записей в RAW: {res}")

def validate_result():
    """Проверяем, что витрина рассчитана и количество строк корректно."""
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='changeme')
    res = client.query('SELECT count() FROM equipment_health_mart').result_rows[0][0]
    
    # У нас 329 единиц оборудования
    if res < 300:
        raise ValueError(f"ОШИБКА: Витрина рассчитана неверно. Получено строк: {res}, ожидалось ~329.")
    print(f"Валидация пройдена! Строк в витрине: {res}")

# Настройки DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                       
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'technoprom_daily_pipeline',
    default_args=default_args,
    description='Ежедневный ETL пайплайн TechnoProm',
    schedule_interval='0 10 * * *', 
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['technoprom', 'etl'],
) as dag:

    # 1. Проверка источников
    check_sources_task = PythonOperator(
        task_id='check_sources',
        python_callable=check_sources,
    )

    # 2. Извлечение (в нашем случае это триггер Spark-джобы, которая читает из Клика)
    extract_task = BashOperator(
        task_id='extract_from_clickhouse',
        bash_command='echo "Извлечение данных из ClickHouse инициировано..."',
    )

    # 3. Построение фичей (Запуск Spark через Docker CLI)
    build_features_task = BashOperator(
        task_id='build_equipment_features',
        bash_command="""
        docker exec technoprom-spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/.ivy \
          --packages com.clickhouse:clickhouse-jdbc:0.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.httpcomponents.client5:httpclient5:5.2.1 \
          /opt/spark/jobs/equipment_health.py
        """,
    )

    # 4. Валидация результата
    validate_task = PythonOperator(
        task_id='validate_result',
        python_callable=validate_result,
    )

    # 5. Выгрузка в MinIO
    upload_task = BashOperator(
        task_id='upload_to_minio',
        bash_command='python /opt/airflow/scripts/upload_to_minio.py',
    )

    check_sources_task >> extract_task >> build_features_task >> validate_task >> upload_task