# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Spark Anti-Patterns
# MAGIC
# MAGIC **Purpose:** Demonstrate the intentionally bad patterns from the original project.
# MAGIC
# MAGIC **Important:** Several examples are intentionally expensive or driver-dangerous. They are shown as code for analysis, but are **not executed by default**.
# MAGIC
# MAGIC Especially do not run `collect()` or `toPandas()` on the full 30M-row encounters table.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE spark_optimization")

patients_df = spark.table("patients")
encounters_df = spark.table("encounters")
doctors_df = spark.table("doctors")
hospitals_df = spark.table("hospitals")
diagnoses_df = spark.table("diagnoses")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 1 — Wide joins + select("*")
# MAGIC
# MAGIC **Problem:** Carries unnecessary columns through multiple joins.

# COMMAND ----------

bad_1 = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .join(diagnoses_df, "diagnosis_code")
    .select("*")
)

bad_1.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 2 — Filter too late

# COMMAND ----------

bad_2 = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .filter(F.col("encounter_date") >= "2026-01-01")
)

bad_2.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 3 — Repartition repeatedly
# MAGIC
# MAGIC Every `repartition()` can introduce a shuffle.

# COMMAND ----------

bad_3 = encounters_df

bad_3 = bad_3.repartition(500)
bad_3 = bad_3.join(patients_df, "patient_id")
bad_3 = bad_3.repartition(300)
bad_3 = bad_3.join(doctors_df, "doctor_id")
bad_3 = bad_3.repartition(200)
bad_3 = bad_3.groupBy("hospital_id").count()

bad_3.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 4 — Repartition before filtering

# COMMAND ----------

bad_4 = (
    encounters_df
    .repartition(500)
    .filter("status = 'Completed'")
)

bad_4.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 5 — Full distinct
# MAGIC
# MAGIC `distinct()` generally requires a shuffle because Spark must bring equal rows together.

# COMMAND ----------

bad_5 = encounters_df.distinct()
bad_5.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 6 — Full-row dropDuplicates
# MAGIC
# MAGIC This uses essentially every encounter column as the duplicate key.

# COMMAND ----------

bad_6 = encounters_df.dropDuplicates([
    "encounter_id",
    "patient_id",
    "encounter_date",
    "encounter_type",
    "doctor_id",
    "hospital_id",
    "diagnosis_code",
    "status"
])

bad_6.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 7 — Global orderBy
# MAGIC
# MAGIC Global sorting can require a large shuffle.

# COMMAND ----------

bad_7 = encounters_df.orderBy("encounter_date")
bad_7.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 8 — collect()
# MAGIC
# MAGIC **DO NOT EXECUTE on the full 30M-row table.**
# MAGIC
# MAGIC It attempts to bring the result to the driver.

# COMMAND ----------

# data = encounters_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 9 — toPandas()
# MAGIC
# MAGIC **DO NOT EXECUTE on the full 30M-row table.**
# MAGIC
# MAGIC It transfers data to the driver and can exhaust driver memory.

# COMMAND ----------

# pdf = encounters_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 10 — Repeated actions
# MAGIC
# MAGIC Each `count()` is an action. Without appropriate reuse/persistence, Spark may repeatedly scan and process the source.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC print(encounters_df.count())
# MAGIC print(encounters_df.filter("status = 'Completed'").count())
# MAGIC print(encounters_df.filter("status = 'Cancelled'").count())
# MAGIC print(encounters_df.filter("status = 'In Progress'").count())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 11 — Cache everything
# MAGIC
# MAGIC Caching every table can consume memory without providing useful reuse.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC patients_df.cache()
# MAGIC encounters_df.cache()
# MAGIC doctors_df.cache()
# MAGIC hospitals_df.cache()
# MAGIC diagnoses_df.cache()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 12 — Recompute the same filtered dataset repeatedly
# MAGIC
# MAGIC If the same expensive intermediate is reused many times, controlled persistence may be appropriate. It should be measured, not assumed.

# COMMAND ----------

completed = encounters_df.filter("status = 'Completed'")

result1 = completed.groupBy("hospital_id").count()
result2 = completed.groupBy("doctor_id").count()
result3 = completed.groupBy("diagnosis_code").count()

result1.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 13 — Join the large table without a clear need

# COMMAND ----------

bad_13 = encounters_df.join(hospitals_df, "hospital_id")
bad_13.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 14 — Filter after all joins

# COMMAND ----------

bad_14 = (
    encounters_df
    .join(patients_df, "patient_id")
    .join(doctors_df, "doctor_id")
    .join(hospitals_df, "hospital_id")
    .join(diagnoses_df, "diagnosis_code")
    .filter("status = 'Completed'")
    .select("province", "hospital_name", "speciality", "category")
)

bad_14.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 15 — Group by almost every column

# COMMAND ----------

bad_15 = (
    encounters_df
    .groupBy(
        "patient_id", "doctor_id", "hospital_id",
        "diagnosis_code", "encounter_type",
        "status", "encounter_date"
    )
    .count()
)

bad_15.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 16 — Repartition by skewed key without measuring the effect

# COMMAND ----------

bad_16 = encounters_df.repartition("hospital_id")
bad_16.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 17 — Aggregation on the skewed key
# MAGIC
# MAGIC This is not inherently "wrong", but it becomes a useful diagnostic experiment because the data is intentionally skewed.

# COMMAND ----------

bad_17 = encounters_df.groupBy("hospital_id").count()
bad_17.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## BAD QUERY 30 — Everything at once
# MAGIC
# MAGIC This intentionally combines several expensive operations. Do not use it as a production query.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "5000")
# MAGIC
# MAGIC result = (
# MAGIC     encounters_df
# MAGIC     .repartition(1000)
# MAGIC     .join(patients_df, "patient_id")
# MAGIC     .repartition(800)
# MAGIC     .join(doctors_df, "doctor_id")
# MAGIC     .repartition(600)
# MAGIC     .join(hospitals_df, "hospital_id")
# MAGIC     .repartition(500)
# MAGIC     .join(diagnoses_df, "diagnosis_code")
# MAGIC     .filter(F.col("encounter_date") >= "2025-01-01")
# MAGIC     .dropDuplicates()
# MAGIC     .groupBy(
# MAGIC         "province", "hospital_name", "doctor_name",
# MAGIC         "speciality", "diagnosis_description", "category"
# MAGIC     )
# MAGIC     .count()
# MAGIC     .orderBy(F.desc("count"))
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Why it is problematic:** repeated repartitions, very high shuffle partition count, late filtering, deduplication and global ordering all increase work.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anti-pattern notebook complete
# MAGIC
# MAGIC **Next notebook:** `08_Performance_Comparison`