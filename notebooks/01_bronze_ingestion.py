
# Bronze Layer: Raw ingestion from Parquet (uploaded by Airflow) into a Delta table.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Configurable input path — Airflow passes this when triggering the notebook.
dbutils.widgets.text("input_path", "dbfs:/FileStore/csj_pipeline/raw_funding.parquet", "Input Parquet Path")

# COMMAND ----------

# Read the raw Parquet file uploaded by Airflow's extract task.
input_path = dbutils.widgets.get("input_path")

df_raw = spark.read.parquet(input_path)

print(f"Read {df_raw.count()} rows from {input_path}")

# COMMAND ----------

# Add ingestion metadata
df_bronze = df_raw.withColumn("_ingestion_timestamp", F.current_timestamp())

# COMMAND ----------

# Create the bronze schema if it doesn't exist yet.
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.raw_funding")

# COMMAND ----------

# Quick validation
count = spark.table("bronze.raw_funding").count()
print(f"Bronze ingestion complete: {count} rows -> bronze.raw_funding")
display(spark.table("bronze.raw_funding").limit(5))
