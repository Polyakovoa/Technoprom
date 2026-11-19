from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from py4j.java_gateway import java_import
import datetime

# Константы и схемы
PARTS_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("part_id", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("price", T.DoubleType(), True)
]))

# Инициализация Spark
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

df_tel = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT *, toString(event_datetime) AS event_datetime_str FROM mart_telemetry)",
    properties=jdbc_props
)

max_date_row = df_tel.agg(F.max("event_datetime_str")).collect()[0]
NOW_STR = max_date_row[0]
NOW_TS = datetime.datetime.strptime(NOW_STR, '%Y-%m-%d %H:%M:%S')
print(f"Расчетная текущая дата (max event_datetime): {NOW_TS}")

# 1. Базовые фичи оборудования
df_base = df_eq.select(
    "equipment_id", "factory_id", "criticality", "commissioned_at",
    "last_maintenance_at", "maintenance_interval_days"
).withColumn(
    "equipment_age_days",
    F.datediff(F.to_date(F.lit(NOW_TS)), F.to_date(F.col("commissioned_at")))
).withColumn(
    "critical_equipment",
    F.when(F.col("criticality").isin("high", "critical"), True).otherwise(False)
).withColumn(
    "maintenance_overdue",
    F.when(
        F.to_date(F.lit(NOW_TS)) > F.to_date(F.col("last_maintenance_at")) + F.col("maintenance_interval_days"),
        True
    ).otherwise(False)
)

# 2. Агрегации по телеметрии
w_tel = Window.partitionBy("equipment_id", "sensor_id").orderBy("event_datetime_str")
df_tel_gaps = df_tel.withColumn(
    "prev_dt", F.lag("event_datetime_str").over(w_tel)
).withColumn(
    "gap_hours", (F.unix_timestamp("event_datetime_str") - F.unix_timestamp("prev_dt")) / 3600
)

cutoff_24h_str = (NOW_TS - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
df_tel_24h = df_tel.filter(F.col("event_datetime_str") > cutoff_24h_str).groupBy("equipment_id").agg(
    F.count("*").cast("int").alias("telemetry_events_24h"),
    F.round(F.avg(F.when(F.col("sensor_type") == "temperature", F.col("value"))), 2).alias("avg_temperature_24h"),
    F.round(F.max(F.when(F.col("sensor_type") == "temperature", F.col("value"))), 2).alias("max_temperature_24h"),
    F.round(F.avg(F.when(F.col("sensor_type") == "vibration", F.col("value"))), 2).alias("avg_vibration_24h"),
    F.count(F.when(
        (F.col("sensor_type") == "temperature") & (F.col("status") == "critical"), True
    )).cast("int").alias("temp_crit_count"),
    F.count(F.when(F.col("sensor_type") == "temperature", True)).cast("int").alias("temp_total_count")
).withColumn(
    "temperature_anomaly", F.when(F.col("max_temperature_24h") > 85.0, True).otherwise(False)
).withColumn(
    "high_temperature_frequency",
    F.when((F.col("temp_crit_count") / F.col("temp_total_count")) > 0.1, True).otherwise(False)
).drop("temp_crit_count", "temp_total_count")

df_vib_press = df_tel.groupBy("equipment_id").agg(
    F.max(F.when(F.col("sensor_type") == "vibration", F.col("value"))).alias("max_vib"),
    F.max(F.when(F.col("sensor_type") == "pressure", F.col("value"))).alias("max_press")
).withColumn(
    "vibration_anomaly", F.when(F.col("max_vib") > 12.0, True).otherwise(False)
).withColumn(
    "pressure_anomaly",
    F.when((F.col("max_press") > 16.0) | (F.col("max_press") < 1.0), True).otherwise(False)
).select("equipment_id", "vibration_anomaly", "pressure_anomaly")

df_gaps_agg = df_tel_gaps.groupBy("equipment_id").agg(
    F.max("gap_hours").alias("max_gap")
).withColumn(
    "telemetry_gap_detected", F.when(F.col("max_gap") > 4.0, True).otherwise(False)
).select("equipment_id", "telemetry_gap_detected")

df_active_sensors = df_sensors.filter(F.col("status") == "active").select("equipment_id", "sensor_id")
df_tel_24h_ids = df_tel.filter(F.col("event_datetime_str") > cutoff_24h_str).select("equipment_id", "sensor_id").distinct()
df_missing = df_active_sensors.join(df_tel_24h_ids, on=["equipment_id", "sensor_id"], how="left_anti") \
    .groupBy("equipment_id").count() \
    .withColumn("sensor_data_missing", F.when(F.col("count") > 0, True).otherwise(False)) \
    .select("equipment_id", "sensor_data_missing")

# 3. Агрегации по авариям
df_failures_clean = df_failures.withColumn("detected_ts", F.to_timestamp("detected_at"))
now_str = NOW_TS.strftime('%Y-%m-%d %H:%M:%S')

df_fail_agg = df_failures_clean.groupBy("equipment_id").agg(
    F.count(F.when(
        (F.col("detected_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 7 DAY"))
        & (F.col("severity") == "critical"),
        True
    )).cast("int").alias("critical_events_7d"),
    F.count(F.when(
        (F.col("detected_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 7 DAY"))
        & (F.col("severity") == "warning"),
        True
    )).cast("int").alias("warning_events_7d"),
    F.count(F.when(
        (F.hour("detected_ts").between(22, 23)) | (F.hour("detected_ts").between(0, 6)),
        True
    )).alias("night_fails_raw"),
    F.count(F.when(F.dayofweek("detected_ts").isin(1, 7), True)).alias("weekend_fails_raw")
).withColumn(
    "night_failures", F.when(F.col("night_fails_raw") > 0, True).otherwise(False)
).withColumn(
    "weekend_failures", F.when(F.col("weekend_fails_raw") > 0, True).otherwise(False)
).drop("night_fails_raw", "weekend_fails_raw")

df_repeat_fail = df_failures_clean.filter(
    F.expr(f"detected_ts > CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY")
).groupBy("equipment_id", "failure_type").count() \
 .filter(F.col("count") >= 2).select("equipment_id").distinct() \
 .withColumn("repeated_failure", F.lit(True))

# 4. Агрегации по ремонтам
df_tickets_clean = df_tickets.withColumn(
    "parts", F.from_json("parts_used_json", PARTS_SCHEMA)
).withColumn(
    "parts_cost", F.expr("aggregate(parts, 0D, (acc, p) -> acc + (p.quantity * p.price))")
).withColumn(
    "created_ts", F.to_timestamp("created_at")
).withColumn(
    "finished_ts", F.to_timestamp("finished_at")
)

df_tickets_agg = df_tickets_clean.groupBy("equipment_id").agg(
    F.count(F.when(
        F.col("created_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"),
        True
    )).cast("int").alias("repairs_30d"),
    F.count(F.when(
        F.col("created_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 90 DAY"),
        True
    )).cast("int").alias("repairs_90d"),
    F.sum(F.when(
        F.col("created_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"),
        F.col("downtime_minutes")
    )).cast("double").alias("total_downtime_30d"),
    F.avg("downtime_minutes").alias("avg_repair_duration"),
    F.sum(F.when(
        F.col("created_ts") > F.expr(f"CAST('{now_str}' AS TIMESTAMP) - INTERVAL 30 DAY"),
        F.col("parts_cost")
    )).alias("parts_cost_30d"),
    F.max("finished_ts").alias("last_finished_ts")
).withColumn(
    "days_since_last_repair",
    F.datediff(F.to_date(F.lit(NOW_TS)), F.to_date(F.col("last_finished_ts")))
).withColumn(
    "frequent_breakdown", F.when(F.col("repairs_30d") >= 3, True).otherwise(False)
).withColumn(
    "high_service_cost", F.when(F.col("parts_cost_30d") > 50000, True).otherwise(False)
).withColumn(
    "recently_repaired", F.when(F.col("days_since_last_repair") <= 7, True).otherwise(False)
)

df_after_repair = df_tickets_clean.select("equipment_id", "finished_ts") \
    .join(df_failures_clean.select("equipment_id", "detected_ts"), on="equipment_id") \
    .filter(F.col("detected_ts") > F.col("finished_ts")) \
    .groupBy("equipment_id").count() \
    .withColumn("failure_after_recent_repair", F.lit(True)) \
    .select("equipment_id", "failure_after_recent_repair")

# 5. Сборка витрины
df_mart = df_base \
    .join(df_tel_24h, "equipment_id", "left") \
    .join(df_vib_press, "equipment_id", "left") \
    .join(df_gaps_agg, "equipment_id", "left") \
    .join(df_missing, "equipment_id", "left") \
    .join(df_fail_agg, "equipment_id", "left") \
    .join(df_repeat_fail, "equipment_id", "left") \
    .join(df_tickets_agg, "equipment_id", "left") \
    .join(df_after_repair, "equipment_id", "left")

bool_cols = [
    "temperature_anomaly", "vibration_anomaly", "pressure_anomaly", "repeated_failure",
    "frequent_breakdown", "sensor_data_missing", "telemetry_gap_detected",
    "high_temperature_frequency", "night_failures", "weekend_failures",
    "critical_equipment", "maintenance_overdue", "recently_repaired",
    "failure_after_recent_repair", "high_service_cost"
]
num_cols = [
    "telemetry_events_24h", "critical_events_7d", "warning_events_7d", "repairs_30d",
    "repairs_90d", "total_downtime_30d", "parts_cost_30d", "avg_temperature_24h",
    "max_temperature_24h", "avg_vibration_24h", "days_since_last_repair", "avg_repair_duration"
]

df_mart = df_mart.fillna(0, subset=num_cols).fillna(False, subset=bool_cols)

df_mart = df_mart.withColumn(
    "unstable_equipment",
    F.when(
        F.col("frequent_breakdown") | F.col("temperature_anomaly") | F.col("vibration_anomaly"),
        True
    ).otherwise(False)
)

# 6. Health Score
df_mart = df_mart.withColumn(
    "health_score",
    F.expr("""
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

# 7. Запись в ClickHouse
print("Запись в ClickHouse...")
df_final.write.jdbc(url=jdbc_url, table="equipment_health_mart", mode="append", properties=jdbc_props)

# 8. Запись в MinIO (S3)
print("Запись в MinIO...")
bucket_name = "analytics-results"
today_str = datetime.datetime.now().strftime("%Y_%m_%d")
filename = f"equipment_health_{today_str}.parquet"
tmp_path = f"s3a://{bucket_name}/tmp_output"

df_final.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_path)

java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
java_import(spark._jvm, 'org.apache.hadoop.fs.FileSystem')
java_import(spark._jvm, 'java.net.URI')

hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.FileSystem.get(spark._jvm.URI(f"s3a://{bucket_name}"), hadoop_conf)

src_dir = spark._jvm.Path(tmp_path)
dst_file = spark._jvm.Path(f"s3a://{bucket_name}/{filename}")

for file_status in fs.listStatus(src_dir):
    if file_status.getPath().getName().startswith("part-"):
        fs.rename(file_status.getPath(), dst_file)
        break

fs.delete(src_dir, True)
print(f"Файл успешно сохранен в MinIO: s3a://{bucket_name}/{filename}")
spark.stop()