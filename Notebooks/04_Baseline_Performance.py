# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 — Baseline Performance
# MAGIC
# MAGIC **Purpose:** Establish the original deliberately inefficient query as our baseline.
# MAGIC
# MAGIC **Rule:** Do not optimize this query yet.
# MAGIC
# MAGIC We first want to understand:
# MAGIC - where Spark shuffles data
# MAGIC - which joins are expensive
# MAGIC - where filtering happens
# MAGIC - how the physical plan is constructed
# MAGIC - how long the baseline takes
# MAGIC
# MAGIC **Business requirement:** Patient encounters by province, hospital, doctor speciality and diagnosis category for encounters from 2025 onwards.

# COMMAND ----------

from pyspark.sql import functions as F
import time

spark.sql("USE spark_optimization")

# Load persisted Delta tables
encounters = spark.table("spark_optimization.encounters")
patients = spark.table("spark_optimization.patients")
doctors = spark.table("spark_optimization.doctors")
hospitals = spark.table("spark_optimization.hospitals")
diagnoses = spark.table("spark_optimization.diagnoses")

print("Source tables loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Baseline query — V1
# MAGIC
# MAGIC This intentionally follows the original project logic: joins happen before the date filter and only then does the aggregation occur.

# COMMAND ----------

result_v1 = (
    encounters.alias("e")
    .join(
        patients.alias("p"),
        F.col("e.patient_id") == F.col("p.patient_id")
    )
    .join(
        doctors.alias("d"),
        F.col("e.doctor_id") == F.col("d.doctor_id")
    )
    .join(
        hospitals.alias("h"),
        F.col("e.hospital_id") == F.col("h.hospital_id")
    )
    .join(
        diagnoses.alias("dx"),
        F.col("e.diagnosis_code") == F.col("dx.diagnosis_code")
    )
    .filter(
        F.col("e.encounter_date") >= F.lit("2025-01-01")
    )
    .groupBy(
        F.col("p.province"),
        F.col("h.hospital_name"),
        F.col("d.speciality"),
        F.col("dx.diagnosis_description"),
        F.col("dx.category")
    )
    .agg(
        F.count("*").alias("encounter_count")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspect the physical plan
# MAGIC
# MAGIC Look for `Exchange`, `SortMergeJoin`, scans, filters and the location of aggregation.

# COMMAND ----------

result_v1.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execute and time the baseline
# MAGIC
# MAGIC `show()` materializes the result. We deliberately use `show()` rather than `collect()` or `toPandas()` because the latter can move large amounts of data to the driver.

# COMMAND ----------

start = time.perf_counter()
result_v1.show(20, truncate=False)
elapsed_v1 = time.perf_counter() - start

print(f"V1 elapsed wall-clock time: {elapsed_v1:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Baseline observations
# MAGIC
# MAGIC Record the Spark UI metrics after the query finishes:
# MAGIC
# MAGIC - Job duration
# MAGIC - Number of stages
# MAGIC - Shuffle read
# MAGIC - Shuffle write
# MAGIC - Long-running tasks
# MAGIC - Any obvious partition imbalance
# MAGIC
# MAGIC **Do not invent these numbers. Copy the values from the Spark UI.**
# MAGIC
# MAGIC **Next notebook:** `05_Optimization_Progression`