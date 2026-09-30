# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Skew and AQE
# MAGIC
# MAGIC **Purpose:** Investigate the deliberately skewed `hospital_id` distribution and then examine Adaptive Query Execution (AQE).
# MAGIC
# MAGIC This notebook is where the project moves from ordinary query tuning into distributed-data behaviour.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
import time

spark.sql("USE spark_optimization")

encounters_df = spark.table("encounters")
hospitals_df = spark.table("hospitals")
patients_df = spark.table("patients").select("patient_id", "province")
doctors_df = spark.table("doctors").select("doctor_id", "speciality")
diagnoses_df = spark.table("diagnoses").select(
    "diagnosis_code", "diagnosis_description", "category"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the skew

# COMMAND ----------

skew = (
    encounters_df
    .groupBy("hospital_id")
    .count()
    .orderBy(F.desc("count"))
)

skew.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Measure the largest-key share

# COMMAND ----------

total = encounters_df.count()
largest = skew.first()["count"]

print(f"Total encounters: {total:,}")
print(f"Largest hospital group: {largest:,}")
print(f"Largest group share: {largest / total * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Baseline skew-sensitive aggregation
# MAGIC
# MAGIC Grouping by the skewed key forces records with the same key toward the same logical aggregation partition.

# COMMAND ----------

skew_query = encounters_df.groupBy("hospital_id").count()

skew_query.explain("formatted")

start = time.perf_counter()
skew_query.show(20)
skew_time = time.perf_counter() - start

print(f"Skew aggregation time: {skew_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AQE configuration
# MAGIC
# MAGIC AQE can change parts of a query plan at runtime based on observed statistics.
# MAGIC
# MAGIC We explicitly record the settings before changing them.

# COMMAND ----------

print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print(
    "Skew join enabled:",
    spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Enable AQE and skew-join handling
# MAGIC
# MAGIC This does not magically fix every skewed aggregation. The Spark UI and physical plan must be used to determine what actually changed.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
print(
    "Skew join enabled:",
    spark.conf.get("spark.sql.adaptive.skewJoin.enabled")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AQE experiment on a join
# MAGIC
# MAGIC We use the large encounters table joined to the small hospitals dimension.

# COMMAND ----------

aqe_join = (
    encounters_df
    .join(hospitals_df, "hospital_id")
    .groupBy("hospital_id", "hospital_name")
    .count()
)

aqe_join.explain("formatted")

start = time.perf_counter()
aqe_join.show(20, truncate=False)
aqe_time = time.perf_counter() - start

print(f"AQE join experiment time: {aqe_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Skew-handling discussion — salting
# MAGIC
# MAGIC **Salting is a technique to split a hot key across multiple logical keys.**
# MAGIC
# MAGIC We do not apply a blind salting rewrite to the main business query here because salting requires coordinated changes to both sides of a join and can change the aggregation strategy.
# MAGIC
# MAGIC For this project, first prove that skew exists and use AQE/Spark UI evidence. A controlled salting experiment can be added if the Spark UI shows the skew is still the dominant bottleneck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What to capture
# MAGIC
# MAGIC From the Spark UI, record:
# MAGIC - longest task duration
# MAGIC - median task duration
# MAGIC - shuffle read/write
# MAGIC - number of tasks
# MAGIC - whether AQE changed the executed plan
# MAGIC - whether skew handling split any skewed partitions
# MAGIC
# MAGIC **Next notebook:** `07_Anti_Patterns`
