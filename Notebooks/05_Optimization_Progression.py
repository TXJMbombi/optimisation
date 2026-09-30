# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 — Optimization Progression
# MAGIC
# MAGIC **Purpose:** Improve the baseline progressively rather than applying every optimization at once.
# MAGIC
# MAGIC Versions:
# MAGIC - V2 — Filter early
# MAGIC - V3 — Column pruning
# MAGIC - V4 — Broadcast small dimensions
# MAGIC - V5 — Tune shuffle partitions
# MAGIC
# MAGIC We preserve the same business result.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import time

spark.sql("USE spark_optimization")

patients_df = spark.table("patients")
encounters_df = spark.table("encounters")
doctors_df = spark.table("doctors")
hospitals_df = spark.table("hospitals")
diagnoses_df = spark.table("diagnoses")

def time_show(df, label, rows=20):
    start = time.perf_counter()
    df.show(rows, truncate=False)
    elapsed = time.perf_counter() - start
    print(f"{label}: {elapsed:.2f} seconds")
    return elapsed

# COMMAND ----------

# MAGIC %md
# MAGIC ## V2 — Filter early
# MAGIC
# MAGIC **Problem:** The baseline joins first and filters the large encounters dataset afterwards.
# MAGIC
# MAGIC **Change:** Reduce the large fact dataset before joins.

# COMMAND ----------

encounters_v2 = encounters_df.filter(
    F.col("encounter_date") >= F.lit("2025-01-01")
)

result_v2 = (
    encounters_v2.alias("e")
    .join(
        patients_df.alias("p"),
        F.col("e.patient_id") == F.col("p.patient_id")
    )
    .join(
        doctors_df.alias("d"),
        F.col("e.doctor_id") == F.col("d.doctor_id")
    )
    .join(
        hospitals_df.alias("h"),
        F.col("e.hospital_id") == F.col("h.hospital_id")
    )
    .join(
        diagnoses_df.alias("dx"),
        F.col("e.diagnosis_code") == F.col("dx.diagnosis_code")
    )
    .groupBy(
        F.col("p.province").alias("province"),
        F.col("h.hospital_name"),
        F.col("d.speciality"),
        F.col("dx.diagnosis_description"),
        F.col("dx.category")
    )
    .count()
    .orderBy(
        F.col("province"),
        F.col("hospital_name")
    )
)

result_v2.explain("formatted")

time_v2 = time_show(result_v2, "V2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V3 — Column pruning
# MAGIC
# MAGIC **Problem:** Carrying unnecessary columns through joins increases data volume.
# MAGIC
# MAGIC **Change:** Select only the columns required by the final result and join keys.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

patients_v3 = patients_df.select(
    "patient_id",
    F.col("province").alias("patient_province")
)

doctors_v3 = doctors_df.select(
    "doctor_id",
    "speciality"
)

hospitals_v3 = hospitals_df.select(
    "hospital_id",
    "hospital_name"
)

diagnoses_v3 = diagnoses_df.select(
    "diagnosis_code",
    "diagnosis_description",
    "category"
)

encounters_v3 = (
    encounters_df
    .filter(
        F.col("encounter_date") >= F.lit("2025-01-01")
    )
    .select(
        "patient_id",
        "doctor_id",
        "hospital_id",
        "diagnosis_code"
    )
)

result_v3 = (
    encounters_v3.alias("e")
    .join(
        patients_v3.alias("p"),
        F.col("e.patient_id") == F.col("p.patient_id")
    )
    .join(
        broadcast(doctors_v3).alias("d"),
        F.col("e.doctor_id") == F.col("d.doctor_id")
    )
    .join(
        broadcast(hospitals_v3).alias("h"),
        F.col("e.hospital_id") == F.col("h.hospital_id")
    )
    .join(
        broadcast(diagnoses_v3).alias("dx"),
        F.col("e.diagnosis_code") == F.col("dx.diagnosis_code")
    )
    .select(
        F.col("p.patient_province").alias("province"),
        F.col("h.hospital_name"),
        F.col("d.speciality"),
        F.col("dx.diagnosis_description"),
        F.col("dx.category")
    )
    .groupBy(
        "province",
        "hospital_name",
        "speciality",
        "diagnosis_description",
        "category"
    )
    .count()
    .orderBy(
        "province",
        "hospital_name"
    )
)

result_v3.explain("formatted")

time_v3 = time_show(result_v3, "V3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V4 — Broadcast small dimensions
# MAGIC
# MAGIC **Problem:** Small dimension tables do not necessarily need a full shuffle-based join.
# MAGIC
# MAGIC **Change:** Broadcast the small dimension tables.
# MAGIC
# MAGIC This is an experiment: inspect the physical plan to verify whether Spark chooses broadcast joins.

# COMMAND ----------

result_v4 = (
    encounters_v3
    .join(broadcast(patients_v3), "patient_id")
    .join(broadcast(doctors_v3), "doctor_id")
    .join(broadcast(hospitals_v3), "hospital_id")
    .join(broadcast(diagnoses_v3), "diagnosis_code")
    .groupBy(
        "province",
        "hospital_name",
        "speciality",
        "diagnosis_description",
        "category"
    )
    .count()
    .orderBy("province", "hospital_name")
)

result_v4.explain("formatted")
time_v4 = time_show(result_v4, "V4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## V5 — Tune shuffle partitions
# MAGIC
# MAGIC This setting controls the default number of partitions used by many shuffle operations.
# MAGIC
# MAGIC Because Serverless resources can vary, do not assume a single number is universally optimal. We start with a moderate value and compare using Spark UI evidence.

# COMMAND ----------

original_shuffle = spark.conf.get("spark.sql.shuffle.partitions")
print("Original shuffle partitions:", original_shuffle)

spark.conf.set("spark.sql.shuffle.partitions", "300")
print("Experiment shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# COMMAND ----------

result_v5 = (
    encounters_v3
    .join(broadcast(patients_v3), "patient_id")
    .join(broadcast(doctors_v3), "doctor_id")
    .join(broadcast(hospitals_v3), "hospital_id")
    .join(broadcast(diagnoses_v3), "diagnosis_code")
    .groupBy(
        "province",
        "hospital_name",
        "speciality",
        "diagnosis_description",
        "category"
    )
    .count()
    .orderBy("province", "hospital_name")
)

result_v5.explain("formatted")
time_v5 = time_show(result_v5, "V5")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Progression results
# MAGIC
# MAGIC Replace the placeholders with values from the Spark UI.

# COMMAND ----------

print(f"V2 wall-clock: {time_v2:.2f}s")
print(f"V3 wall-clock: {time_v3:.2f}s")
print(f"V4 wall-clock: {time_v4:.2f}s")
print(f"V5 wall-clock: {time_v5:.2f}s")
print("Remember: wall-clock time alone is not enough; use Spark UI metrics and physical plans.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next notebook:** `06_Skew_And_AQE`