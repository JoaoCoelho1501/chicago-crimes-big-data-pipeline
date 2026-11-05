import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when, avg, sum as spark_sum

spark = SparkSession.builder.appName("Gold Layer").getOrCreate()

SILVER = f"{HDFS_ROOT}/silver/"
GOLD = f"{HDFS_ROOT}/gold"

df = spark.read.parquet(SILVER)

print("Silver lido com sucesso!")

# ── 1. Crimes por tipo e ano ──────────────────────────────────────────
print("A processar: crimes por tipo e ano...")
df.groupBy("year", "crime_type") \
  .count() \
  .orderBy("year", "count", ascending=[True, False]) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_tipo_ano/")

# ── 2. Crimes por distrito ────────────────────────────────────────────
print("A processar: crimes por distrito...")
df.groupBy("district") \
  .count() \
  .orderBy("count", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_distrito/")

# ── 3. Taxa de arrests por tipo de crime ─────────────────────────────
print("A processar: taxa de arrests...")
df.groupBy("crime_type") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("arrest").cast("int")).alias("total_arrests"),
      round(spark_sum(col("arrest").cast("int")) / count("*") * 100, 2).alias("taxa_arrest_pct")
  ) \
  .orderBy("taxa_arrest_pct", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/taxa_arrests_por_tipo/")

# ── 4. Crimes por hora do dia ─────────────────────────────────────────
print("A processar: crimes por hora...")
df.groupBy("crime_hour") \
  .count() \
  .orderBy("crime_hour") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_hora/")

# ── 5. Crimes por dia da semana ───────────────────────────────────────
print("A processar: crimes por dia da semana...")
df.groupBy("day_of_week") \
  .count() \
  .orderBy("day_of_week") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_dia_semana/")

# ── 6. Crimes por mês ─────────────────────────────────────────────────
print("A processar: crimes por mês...")
df.groupBy("year", "crime_month") \
  .count() \
  .orderBy("year", "crime_month") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_mes/")

# ── 7. Top community areas com mais crimes ────────────────────────────
print("A processar: top community areas...")
df.groupBy("community_area") \
  .count() \
  .orderBy("count", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_community_area/")

# ── 8. Crimes por tipo de localização ────────────────────────────────
print("A processar: crimes por localização...")
df.groupBy("location_description") \
  .count() \
  .orderBy("count", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_localizacao/")

# ── 9. Crimes domésticos por ano ──────────────────────────────────────
print("A processar: crimes domésticos por ano...")
df.groupBy("year") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("domestic").cast("int")).alias("total_domesticos"),
      round(spark_sum(col("domestic").cast("int")) / count("*") * 100, 2).alias("pct_domesticos")
  ) \
  .orderBy("year") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_domesticos_por_ano/")

# ── 10. Crimes noturnos vs diurnos vs vespertinos ─────────────────────
print("A processar: crimes por período do dia...")
df.withColumn("periodo",
    when((col("crime_hour") >= 0) & (col("crime_hour") < 6), "Noturno")
    .when((col("crime_hour") >= 6) & (col("crime_hour") < 18), "Diurno")
    .otherwise("Vespertino")
) \
  .groupBy("year", "periodo") \
  .count() \
  .orderBy("year", "periodo") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_periodo_dia/")

# ── 11. Taxa de arrests por distrito ─────────────────────────────────
print("A processar: taxa de arrests por distrito...")
df.groupBy("district") \
  .agg(
      count("*").alias("total"),
      spark_sum(col("arrest").cast("int")).alias("total_arrests"),
      round(spark_sum(col("arrest").cast("int")) / count("*") * 100, 2).alias("taxa_arrest_pct")
  ) \
  .orderBy("taxa_arrest_pct", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/taxa_arrests_por_distrito/")

# ── 12. Evolução anual por tipo de crime ──────────────────────────────
print("A processar: evolução anual por tipo de crime...")
df.groupBy("year", "crime_type") \
  .count() \
  .orderBy("crime_type", "year") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/evolucao_crime_por_ano/")

# ── 13. Crimes por ward ───────────────────────────────────────────────
print("A processar: crimes por ward...")
df.groupBy("ward") \
  .count() \
  .orderBy("count", ascending=False) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_por_ward/")

# ── 14. Crime type + hora do dia ──────────────────────────────────────
print("A processar: crime type por hora...")
df.groupBy("crime_type", "crime_hour") \
  .count() \
  .orderBy("crime_type", "crime_hour") \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/crimes_tipo_por_hora/")

# ── 15. Top 10 crimes mais frequentes por ano ─────────────────────────
print("A processar: top crimes por ano...")
df.groupBy("year", "crime_type") \
  .count() \
  .orderBy("year", "count", ascending=[True, False]) \
  .write.mode("overwrite") \
  .parquet(f"{GOLD}/top_crimes_por_ano/")

print("\nGold Layer concluída! Todas as métricas foram guardadas.")
