import os
HDFS_ROOT = os.environ.get("HDFS_ROOT", "hdfs://hadoop-master:39000/<utilizador>")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, month, hour, dayofweek

spark = SparkSession.builder.appName("Silver Layer").getOrCreate()

# 1. Ler o Bronze
df = spark.read.json(f"{HDFS_ROOT}/bronze/")

# 2. Selecionar e renomear colunas
df_silver = df.select(
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

# 3. Adicionar colunas derivadas
df_silver = df_silver.withColumn("crime_month", month(col("crime_date"))) \
                     .withColumn("crime_hour", hour(col("crime_date"))) \
                     .withColumn("day_of_week", dayofweek(col("crime_date")))

# 4. Preencher location_description nulo com UNKNOWN
df_silver = df_silver.fillna({"location_description": "UNKNOWN"})

# 5. Apagar linhas com nulos em latitude, longitude, ward e community_area
df_silver = df_silver.dropna(subset=["latitude", "longitude", "ward", "community_area"])

# 6. Apagar linhas com nulos nas colunas essenciais
df_silver = df_silver.dropna(subset=["crime_id", "crime_type", "crime_date"])

print(f"Total de registos após limpeza: {df_silver.count()}")

# 7. Guardar no Silver em Parquet particionado por ano
df_silver.write.partitionBy("year") \
    .mode("overwrite") \
    .parquet(f"{HDFS_ROOT}/silver/")

print("Silver Layer concluída!")
