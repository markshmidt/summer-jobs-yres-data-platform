
# Silver Layer: Clean, validate, and type-cast the raw bronze data.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# COMMAND ----------

# Read raw data from the bronze Delta table
df_bronze = spark.table("bronze.raw_funding")

print(f"Read {df_bronze.count()} rows from bronze.raw_funding")

# COMMAND ----------

# Rename bilingual columns to clean English snake_case names
df_renamed = (
    df_bronze
    .withColumnRenamed("Program Year / Année du programme", "program_year")
    .withColumnRenamed("Region / Région", "region")
    .withColumnRenamed("Activity Constituency", "riding")
    .withColumnRenamed("Organization Common Name / Nom commun de l'organisme", "organization_name")
    .withColumnRenamed("Amount Paid / Montant payé", "amount_paid")
    .withColumnRenamed("Confirmed Jobs Created / Emplois confirmés créés", "jobs_created")
)

# COMMAND ----------

# Cast string columns to proper numeric types
df_typed = (
    df_renamed
    .withColumn("program_year", F.col("program_year").cast(IntegerType()))
    .withColumn("amount_paid", F.col("amount_paid").cast(DoubleType()))
    .withColumn("jobs_created", F.col("jobs_created").cast(IntegerType()))
)

# COMMAND ----------

# Compute average salary per job (funding / jobs created).
# Null when jobs_created is 0 to avoid division by zero.
df_with_salary = df_typed.withColumn(
    "avg_salary",
    F.when(F.col("jobs_created") > 0, F.col("amount_paid") / F.col("jobs_created"))
)

# COMMAND ----------

# --- Data quality checks ---
# Flag rows that have issues but keep them in the table (soft deletes, not hard).
# The _dq_flag column marks problematic rows for downstream filtering.

df_flagged = (
    df_with_salary
    .withColumn("_dq_flags", F.array())

    # Null program year — likely a parsing issue
    .withColumn("_dq_flags", F.when(
        F.col("program_year").isNull(),
        F.array_union(F.col("_dq_flags"), F.array(F.lit("null_year")))
    ).otherwise(F.col("_dq_flags")))

    # Null or negative funding
    .withColumn("_dq_flags", F.when(
        F.col("amount_paid").isNull() | (F.col("amount_paid") < 0),
        F.array_union(F.col("_dq_flags"), F.array(F.lit("invalid_funding")))
    ).otherwise(F.col("_dq_flags")))

    # Zero funding but jobs were created — suspicious
    .withColumn("_dq_flags", F.when(
        (F.col("amount_paid") == 0) & (F.col("jobs_created") > 0),
        F.array_union(F.col("_dq_flags"), F.array(F.lit("zero_funding_with_jobs")))
    ).otherwise(F.col("_dq_flags")))

    # Avg salary outlier — over $50k per summer job is suspicious
    .withColumn("_dq_flags", F.when(
        F.col("avg_salary") > 50000,
        F.array_union(F.col("_dq_flags"), F.array(F.lit("salary_outlier")))
    ).otherwise(F.col("_dq_flags")))
)

# COMMAND ----------

# Log data quality summary
total = df_flagged.count()
flagged = df_flagged.filter(F.size("_dq_flags") > 0).count()

print(f"Data quality: {flagged} of {total} rows flagged ({flagged/total*100:.1f}%)")

# Break down by flag type
df_flagged.filter(F.size("_dq_flags") > 0) \
    .select(F.explode("_dq_flags").alias("flag")) \
    .groupBy("flag").count() \
    .orderBy(F.desc("count")) \
    .show()

# COMMAND ----------

# Write clean data to silver Delta table
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

df_flagged.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver.clean_funding")

# COMMAND ----------

# Quick validation
count = spark.table("silver.clean_funding").count()
clean = spark.table("silver.clean_funding").filter(F.size("_dq_flags") == 0).count()
print(f"Silver complete: {count} total rows, {clean} clean rows -> silver.clean_funding")
display(spark.table("silver.clean_funding").limit(5))
