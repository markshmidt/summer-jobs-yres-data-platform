
# Bronze Layer: Validation notebook.
# Data is loaded into bronze.raw_funding by Airflow via the SQL Statement API.
# Run this notebook manually to inspect the bronze table.

# COMMAND ----------

# Check row count and schema
df = spark.table("bronze.raw_funding")
print(f"Rows: {df.count()}")
df.printSchema()

# COMMAND ----------

# Preview data
display(spark.table("bronze.raw_funding").limit(10))

# COMMAND ----------

# Check ingestion timestamps — when was data last loaded?
from pyspark.sql import functions as F
spark.table("bronze.raw_funding") \
    .select(
        F.min("_ingestion_timestamp").alias("earliest"),
        F.max("_ingestion_timestamp").alias("latest"),
    ).show(truncate=False)
