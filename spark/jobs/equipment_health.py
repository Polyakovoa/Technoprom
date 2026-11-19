from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max as _max, min as _min, count, when, round, lit, 
    datediff, to_date, sum as _sum, hour, dayofweek, lag,
    from_json, expr, coalesce, unix_timestamp, isnull, to_timestamp
)
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder \
    .appName("EquipmentHealthMart") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

jdbc_url = "jdbc:clickhouse://clickhouse:8123/default"
jdbc_props = {"user": "default", "password": "changeme", "driver": "com.clickhouse.jdbc.ClickHouseDriver"}

print("Чтение данных из ClickHouse...")
df_eq = spark.read.jdbc(url=jdbc_url, table="raw_equipment", properties=jdbc_props)
df_sensors = spark.read.jdbc(url=jdbc_url, table="raw_sensors", properties=jdbc_props)
df_failures = spark.read.jdbc(url=jdbc_url, table="raw_failures", properties=jdbc_props)
df_tickets = spark.read.jdbc(url=jdbc_url, table="raw_repair_tickets", properties=jdbc_props)

# Читаем телеметрию, конвертируя DateTime в String на стороне ClickHouse во избежание проблем с миллисекундами
df_tel = spark.read.jdbc(url=jdbc_url, table="(SELECT *, toString(event_datetime) AS event_datetime_str FROM mart_telemetry)", properties=jdbc_props)

# Определяем "текущую" дату
max_date_row = df_tel.agg(_max("event_datetime_str")).collect()[0]
NOW_STR = max_date_row[0]
NOW_TS = datetime.datetime.strptime(NOW_STR, '%Y-%m-%d %H:%M:%S')
print(f"Расчетная текущая дата (max event_datetime): {NOW_TS}")

# --- 1. Базовые фичи оборудования ---
df_base = df_eq.select(
    "equipment_id", "factory_id", "criticality", "commissioned_at", "last_maintenance_at", "maintenance_interval_days"
).withColumn(
    "equipment_age_days", datediff(to_date(lit(NOW_TS)), to_date(col("commissioned_at")))
).withColumn(
    "critical_equipment", when(col("criticality").isin("high", "critical"), True).otherwise(False)
).withColumn(
    "maintenance_overdue", 
    when(to_date(lit(NOW_TS)) > to_date(col("last_maintenance_at")) + col("maintenance_interval_days"), True).otherwise(False)
)

# --- 2. Агрегации по телеметрии ---
w_tel = Window.partitionBy("equipment_id", "sensor_id").orderBy("event_datetime_str")
df_tel_gaps = df_tel.withColumn(
    "prev_dt", lag("event_datetime_str").over(w_tel)
).withColumn(
    "gap_hours", (unix_timestamp("event_datetime_str") - unix_timestamp("prev_dt")) / 3600
)

cutoff_24h_str = (NOW_TS - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
df_tel_24h = df_tel.filter(col("event_datetime_str") > cutoff_24h_str).groupBy("equipment_id").agg(
    count("*").cast("int").alias("telemetry_events_24h"),
    round(avg(when(col("sensor_type") == "temperature", col("value"))), 2).alias("avg_temperature_24h"),
    round(_max(when(col("sensor_type") == "temperature", col("value"))), 2).alias("max_temperature_24h"),
    round(avg(when(col("sensor_type") == "vibration", col("value"))), 2).alias("avg_vibration_24h"),
    count(when((col("sensor_type") == "temperature") & (col("status") == "critical"), True)).cast("int").alias("temp_crit_count"),
    count(when(col("sensor_type") == "temperature", True)).cast("int").alias("temp_total_count")
).withColumn(
    "temperature_anomaly", when(col("max_temperature_24h") > 85.0, True).otherwise(False)
).withColumn(
    "high_temperature_frequency", when((col("temp_crit_count") / col("temp_total_count")) > 0.1, True).otherwise(False)
).drop("temp_crit_count", "temp_total_count")

df_vib_press = df_tel.groupBy("equipment_id").agg(
    _max(when(col("sensor_type") == "vibration", col("value"))).alias("max_vib"),
    _max(when(col("sensor_type") == "pressure", col("value"))).alias("max_press")
).withColumn(
    "vibration_anomaly", when(col("max_vib") > 12.0, True).otherwise(False)
).withColumn(
    "pressure_anomaly", when((col("max_press") > 16.0) | (col("max_press") < 1.0), True).otherwise(False)
).select("equipment_id", "vibration_anomaly", "pressure_anomaly")

df_gaps_agg = df_tel_gaps.groupBy("equipment_id").agg(
    _max("gap_hours").alias("max_gap")
).withColumn(
    "telemetry_gap_detected", when(col("max_gap") > 4.0, True).otherwise(False)
).select("equipment_id", "telemetry_gap_detected")

df_active_sensors = df_sensors.filter(col("status") == "active").select("equipment_id", "sensor_id")
df_tel_24h_ids = df_tel.filter(col("event_datetime_str") > cutoff_24h_str).select("equipment_id", "sensor_id").distinct()
df_missing = df_active_sensors.join(df_tel_24h_ids, on=["equipment_id", "sensor_id"], how="left_anti") \
    .groupBy("equipment_id").count() \
    .withColumn("sensor_data_missing", when(col("count") > 0, True).otherwise(False)) \
    .select("equipment_id", "sensor_data_missing")

# --- 3. Агрегации по авариям ---
df_failures_clean = df_failures.withColumn("detected_ts", to_timestamp("detected_at"))
now_str = NOW_TS.strftime('%Y-%m-%d %H:%M:%S')

df_fail_agg = df_failures_clean.groupBy("equipment_id").agg(
    count(when((col("detected_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 7 DAY")) & (col("severity") == "critical"), True)).cast("int").alias("critical_events_7d"),
    count(when((col("detected_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 7 DAY")) & (col("severity") == "warning"), True)).cast("int").alias("warning_events_7d"),
    count(when((hour("detected_ts").between(22, 23)) | (hour("detected_ts").between(0, 6)), True)).alias("night_fails_raw"),
    count(when(dayofweek("detected_ts").isin(1, 7), True)).alias("weekend_fails_raw")
).withColumn("night_failures", when(col("night_fails_raw") > 0, True).otherwise(False)) \
 .withColumn("weekend_failures", when(col("weekend_fails_raw") > 0, True).otherwise(False)) \
 .drop("night_fails_raw", "weekend_fails_raw")

df_repeat_fail = df_failures_clean.filter(expr(f"detected_ts > CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY")) \
    .groupBy("equipment_id", "failure_type").count() \
    .filter(col("count") >= 2).select("equipment_id").distinct() \
    .withColumn("repeated_failure", lit(True))

# --- 4. Агрегации по ремонтам ---
parts_schema = ArrayType(StructType([
    StructField("part_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True)
]))

df_tickets_clean = df_tickets.withColumn(
    "parts", from_json("parts_used_json", parts_schema)
).withColumn(
    "parts_cost", expr("aggregate(parts, 0D, (acc, p) -> acc + (p.quantity * p.price))")
).withColumn(
    "created_ts", to_timestamp("created_at")
).withColumn(
    "finished_ts", to_timestamp("finished_at")
)

df_tickets_agg = df_tickets_clean.groupBy("equipment_id").agg(
    count(when(col("created_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"), True)).cast("int").alias("repairs_30d"),
    count(when(col("created_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 90 DAY"), True)).cast("int").alias("repairs_90d"),
    _sum(when(col("created_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"), col("downtime_minutes"))).cast("double").alias("total_downtime_30d"),
    avg("downtime_minutes").alias("avg_repair_duration"),
    _sum(when(col("created_ts") > expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"), col("parts_cost"))).alias("parts_cost_30d"),
    _max("finished_ts").alias("last_finished_ts")
).withColumn(
    "days_since_last_repair", datediff(to_date(lit(NOW_TS)), to_date(col("last_finished_ts")))
).withColumn(
    "frequent_breakdown", when(col("repairs_30d") >= 3, True).otherwise(False)
).withColumn(
    "high_service_cost", when(col("parts_cost_30d") > 50000, True).otherwise(False)
).withColumn(
    "recently_repaired", when(col("days_since_last_repair") <= 7, True).otherwise(False)
)

df_after_repair = df_tickets_clean.select("equipment_id", "finished_ts") \
    .join(df_failures_clean.select("equipment_id", "detected_ts"), on="equipment_id") \
    .filter(col("detected_ts") > col("finished_ts")) \
    .groupBy("equipment_id").count() \
    .withColumn("failure_after_recent_repair", lit(True)) \
    .select("equipment_id", "failure_after_recent_repair")

# --- 5. Сборка ---
df_mart = df_base \
    .join(df_tel_24h, "equipment_id", "left") \
    .join(df_vib_press, "equipment_id", "left") \
    .join(df_gaps_agg, "equipment_id", "left") \
    .join(df_missing, "equipment_id", "left") \
    .join(df_fail_agg, "equipment_id", "left") \
    .join(df_repeat_fail, "equipment_id", "left") \
    .join(df_tickets_agg, "equipment_id", "left") \
    .join(df_after_repair, "equipment_id", "left")

bool_cols = ["temperature_anomaly", "vibration_anomaly", "pressure_anomaly", "repeated_failure", 
             "frequent_breakdown", "sensor_data_missing", "telemetry_gap_detected", 
             "high_temperature_frequency", "night_failures", "weekend_failures", 
             "critical_equipment", "maintenance_overdue", "recently_repaired", 
             "failure_after_recent_repair", "high_service_cost"]
num_cols = ["telemetry_events_24h", "critical_events_7d", "warning_events_7d", "repairs_30d", 
            "repairs_90d", "total_downtime_30d", "parts_cost_30d", "avg_temperature_24h", 
            "max_temperature_24h", "avg_vibration_24h", "days_since_last_repair", "avg_repair_duration"]

df_mart = df_mart.fillna(0, subset=num_cols).fillna(False, subset=bool_cols)

df_mart = df_mart.withColumn("unstable_equipment", 
    when(col("frequent_breakdown") | col("temperature_anomaly") | col("vibration_anomaly"), True).otherwise(False)
)

# --- 6. Health Score ---
df_mart = df_mart.withColumn("health_score", 
    expr("""
        greatest(0, least(100, 
            100 
            - (CASE WHEN temperature_anomaly THEN 20 ELSE 0 END)
            - (CASE WHEN frequent_breakdown THEN 15 ELSE 0 END)
            - (CASE WHEN maintenance_overdue THEN 10 ELSE 0 END)
            - (CASE WHEN repeated_failure THEN 15 ELSE 0 END)
            - (CASE WHEN vibration_anomaly THEN 15 ELSE 0 END)
            - (CASE WHEN telemetry_gap_detected THEN 10 ELSE 0 END)
            - (CASE WHEN unstable_equipment THEN 10 ELSE 0 END)
        ))
    """)
)

final_cols = [
    "equipment_id", "factory_id", "equipment_age_days", "telemetry_events_24h",
    "avg_temperature_24h", "max_temperature_24h", "avg_vibration_24h",
    "temperature_anomaly", "vibration_anomaly", "pressure_anomaly",
    "critical_events_7d", "warning_events_7d", "repairs_30d", "repairs_90d",
    "days_since_last_repair", "total_downtime_30d", "avg_repair_duration",
    "parts_cost_30d", "repeated_failure", "frequent_breakdown",
    "sensor_data_missing", "telemetry_gap_detected", "high_temperature_frequency",
    "night_failures", "weekend_failures", "critical_equipment", "maintenance_overdue",
    "recently_repaired", "failure_after_recent_repair", "high_service_cost",
    "unstable_equipment", "health_score"
]

df_final = df_mart.select(*final_cols)

# --- 7. Запись в ClickHouse ---
print("Запись в ClickHouse...")
df_final.write.jdbc(url=jdbc_url, table="equipment_health_mart", mode="append", properties=jdbc_props)

# --- 8. Запись в MinIO (S3) ---
print("Запись в MinIO...")
bucket_name = "analytics-results"
today_str = datetime.datetime.now().strftime("%Y_%m_%d")
filename = f"equipment_health_{today_str}.parquet"
tmp_path = f"s3a://{bucket_name}/tmp_output"

df_final.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_path)

# --- ИСПРАВЛЕННЫЙ БЛОК ПЕРЕИМЕНОВАНИЯ ---
from py4j.java_gateway import java_import
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
java_import(spark._jvm, 'java.net.URI')

hadoop_conf = spark._jsc.hadoopConfiguration()
# Явно получаем S3A файловую систему
fs = spark._jvm.FileSystem.get(spark._jvm.URI(f"s3a://{bucket_name}"), hadoop_conf)

src_dir = spark._jvm.Path(tmp_path)
dst_file = spark._jvm.Path(f"s3a://{bucket_name}/{filename}")

for file_status in fs.listStatus(src_dir):
    if file_status.getPath().getName().startswith("part-"):
        fs.rename(file_status.getPath(), dst_file)
        break

# Удаляем временную папку
fs.delete(src_dir, True)
print(f"✅ Файл успешно сохранен в MinIO: s3a://{bucket_name}/{filename}")