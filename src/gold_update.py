import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when, sum as spark_sum
from datetime import datetime

spark = SparkSession.builder.appName("Gold Update").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

SILVER = f"{HDFS_ROOT}/silver/"
GOLD = f"{HDFS_ROOT}/gold"

current_year = datetime.now().year

print(f"A recalcular Gold para o ano {current_year}...")

# Ler só o ano atual do Silver
df = spark.read.parquet(f"{SILVER}").filter(f"year = {current_year}")

print(f"Registos Silver {current_year}: {df.count()}")

# ── Recalcular todas as métricas apenas para o ano atual ──────────────

df.groupBy("year", "crime_type") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_tipo_ano/year={current_year}/")

df.groupBy("district") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_distrito/year={current_year}/")

df.groupBy("crime_type") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("arrest").cast("int")).alias("total_arrests"),
      round(spark_sum(col("arrest").cast("int")) / count("*") * 100, 2).alias("taxa_arrest_pct")
  ) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/taxa_arrests_por_tipo/year={current_year}/")

df.groupBy("crime_hour") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_hora/year={current_year}/")

df.groupBy("day_of_week") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_dia_semana/year={current_year}/")

df.groupBy("year", "crime_month") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_mes/year={current_year}/")

df.groupBy("community_area") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_community_area/year={current_year}/")

df.groupBy("location_description") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_localizacao/year={current_year}/")

df.groupBy("year") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("domestic").cast("int")).alias("total_domesticos"),
      round(spark_sum(col("domestic").cast("int")) / count("*") * 100, 2).alias("pct_domesticos")
  ) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_domesticos_por_ano/year={current_year}/")

df.withColumn("periodo",
    when((col("crime_hour") >= 0) & (col("crime_hour") < 6), "Noturno")
    .when((col("crime_hour") >= 6) & (col("crime_hour") < 18), "Diurno")
    .otherwise("Vespertino")
) \
  .groupBy("year", "periodo") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_periodo_dia/year={current_year}/")

df.groupBy("district") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("arrest").cast("int")).alias("total_arrests"),
      round(spark_sum(col("arrest").cast("int")) / count("*") * 100, 2).alias("taxa_arrest_pct")
  ) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/taxa_arrests_por_distrito/year={current_year}/")

df.groupBy("ward") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_ward/year={current_year}/")

df.groupBy("crime_type", "crime_hour") \
  .count() \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_tipo_por_hora/year={current_year}/")

print(f"\nGold atualizado para {current_year} com sucesso!")
spark.stop()
