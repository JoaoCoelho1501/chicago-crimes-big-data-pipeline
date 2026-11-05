import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, hour, dayofweek
from datetime import datetime
import os
from datetime import datetime

spark = SparkSession.builder.appName("Silver Incremental").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

HDFS_BASE = f"{HDFS_ROOT}"
LAST_UPDATE_FILE = os.path.expanduser("~/last_update.txt")

# ── Ler a data da última atualização ─────────────────────────────────

if not os.path.exists(LAST_UPDATE_FILE):
    print("Ficheiro last_update.txt não encontrado — sem dados novos para processar.")
    spark.stop()
    exit(0)

with open(LAST_UPDATE_FILE, "r") as f:
    last_update = f.read().strip()

print(f"A processar dados novos desde: {last_update}")

# ── Ler só o ano atual do Bronze ──────────────────────────────────────
current_year = datetime.now().year
df = spark.read.json(f"{HDFS_BASE}/bronze/year={current_year}/")

print(f"Registos lidos do Bronze {current_year}: {df.count()}")

# ── Filtrar só os registos novos ──────────────────────────────────────
df_new = df.filter(col("date") > last_update)

if df_new.count() == 0:
    print("Sem dados novos para processar no Silver.")
    spark.stop()
    exit(0)

print(f"Registos novos a processar: {df_new.count()}")

# ── Transformações ────────────────────────────────────────────────────
df_silver = df_new.select(
    col("id").alias("crime_id"),
    col("primary_type").alias("crime_type"),
    col("description"),
    col("location_description"),
    col("arrest").cast("boolean"),
    col("domestic").cast("boolean"),
    col("district").cast("int"),
    col("ward").cast("int"),
    col("community_area").cast("int"),
    col("year").cast("int"),
    col("latitude").cast("double"),
    col("longitude").cast("double"),
    to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("crime_date")
)

df_silver = df_silver.withColumn("crime_month", month(col("crime_date"))) \
                     .withColumn("crime_hour", hour(col("crime_date"))) \
                     .withColumn("day_of_week", dayofweek(col("crime_date")))

df_silver = df_silver.fillna({"location_description": "UNKNOWN"})
df_silver = df_silver.dropna(subset=["latitude", "longitude", "ward", "community_area"])
df_silver = df_silver.dropna(subset=["crime_id", "crime_type", "crime_date"])

print(f"Registos após limpeza: {df_silver.count()}")

# ── Adicionar ao Silver (append, não overwrite) ───────────────────────
df_silver.write.partitionBy("year") \
    .mode("append") \
    .parquet(f"{HDFS_BASE}/silver/")

print("Silver incremental concluído!")
spark.stop()
