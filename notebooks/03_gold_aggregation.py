
# Gold Layer: Aggregate clean silver data per organization per riding per year

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Read clean data from silver, excluding flagged rows
df_silver = spark.table("silver.clean_funding").filter(F.size("_dq_flags") == 0)

print(f"Read {df_silver.count()} clean rows from silver.clean_funding")

# COMMAND ----------

# Aggregate per org per riding per year.
# Same org can have multiple grants in the same riding/year so we combine them into one row.
df_org = (
    df_silver
    .groupBy("region", "riding", "program_year", "organization_name")
    .agg(
        F.sum("amount_paid").alias("total_funding"),
        F.sum("jobs_created").alias("total_jobs"),
        F.count("*").alias("grant_count"),
    )
    .withColumn("avg_salary",
        F.when(F.col("total_jobs") > 0, F.col("total_funding") / F.col("total_jobs"))
    )
    .orderBy("region", "riding", "program_year", "organization_name")
)

# COMMAND ----------

# Write to gold Delta table
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

df_org.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold.org_funding_summary")

# COMMAND ----------

# Quick validation
count = spark.table("gold.org_funding_summary").count()
regions = spark.table("gold.org_funding_summary").select("region").distinct().count()
years = spark.table("gold.org_funding_summary").select("program_year").distinct().count()
multi_grant = spark.table("gold.org_funding_summary").filter(F.col("grant_count") > 1).count()

print(f"Gold complete: {count} rows ({regions} provinces, {years} years) -> gold.org_funding_summary")
print(f"Orgs with multiple grants combined: {multi_grant}")
display(spark.table("gold.org_funding_summary").limit(10))
