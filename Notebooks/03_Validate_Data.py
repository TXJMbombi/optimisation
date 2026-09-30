# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 — Validate Data
# MAGIC
# MAGIC **Purpose:** Confirm that all five Delta tables exist, have the expected approximate row counts, and have the intended skew.
# MAGIC
# MAGIC We do this before performance testing so that later performance results are based on known-good data.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE spark_optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List project tables

# COMMAND ----------

spark.sql("SHOW TABLES IN spark_optimization").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate row counts
# MAGIC
# MAGIC These counts are actions. Run them once here rather than repeatedly in later notebooks.

# COMMAND ----------

expected = {
    "patients": 2_000_000,
    "encounters": 30_000_000,
    "doctors": 20_000,
    "hospitals": 500,
    "diagnoses": 5_000
}

actual = {}

for table, expected_count in expected.items():
    count = spark.table(table).count()
    actual[table] = count
    print(f"{table:12} actual={count:,} expected={expected_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate schemas

# COMMAND ----------

for table in expected:
    print(f"\n===== {table} =====")
    spark.table(table).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Check key uniqueness in dimension tables

# COMMAND ----------

checks = {
    "patients": "patient_id",
    "hospitals": "hospital_id",
    "doctors": "doctor_id",
    "diagnoses": "diagnosis_id"
}

for table, key in checks.items():
    total = spark.table(table).count()
    distinct_count = spark.table(table).select(key).distinct().count()
    print(f"{table}: total={total:,}, distinct {key}={distinct_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Check encounter skew
# MAGIC
# MAGIC Hospital 1 should contain roughly 55% of encounters.

# COMMAND ----------

encounters_df = spark.table("encounters")

skew = (
    encounters_df
    .groupBy("hospital_id")
    .count()
    .orderBy(F.desc("count"))
)

skew.show(20)

hospital_1 = skew.filter(F.col("hospital_id") == 1).first()["count"]
total_encounters = encounters_df.count()
share = hospital_1 / total_encounters * 100

print(f"Hospital 1 encounters: {hospital_1:,}")
print(f"Hospital 1 share:       {share:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Basic null checks

# COMMAND ----------

columns_to_check = [
    "encounter_id", "patient_id", "encounter_date",
    "doctor_id", "hospital_id", "diagnosis_code", "status"
]

null_exprs = [
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in columns_to_check
]

encounters_df.select(null_exprs).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation complete
# MAGIC
# MAGIC If the row counts, schemas and skew look correct, proceed to `04_Baseline_Performance`.