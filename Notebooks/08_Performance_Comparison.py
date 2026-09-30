# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Performance Comparison
# MAGIC
# MAGIC **Purpose:** Create a single place to record the measured results of the optimization experiments.
# MAGIC
# MAGIC Do not invent values. Copy measured values from the Spark UI.
# MAGIC
# MAGIC This notebook provides the structure for the final comparison.

# COMMAND ----------

spark.sql("USE spark_optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Results template
# MAGIC
# MAGIC Fill these values after each experiment. Runtime can come from your notebook timing, while shuffle metrics should come from the Spark UI.

# COMMAND ----------

results = [
    ("V1", "Baseline", None, None, None, "Original deliberately inefficient query"),
    ("V2", "Filter early", None, None, None, "Filter encounters before joins"),
    ("V3", "Column pruning", None, None, None, "Carry only required columns"),
    ("V4", "Broadcast", None, None, None, "Broadcast small dimensions"),
    ("V5", "Shuffle partitions", None, None, None, "Tune spark.sql.shuffle.partitions"),
    ("V6", "Skew", None, None, None, "Investigate skewed hospital_id"),
    ("V7", "AQE", None, None, None, "Adaptive Query Execution"),
    ("V8", "Cache", None, None, None, "Cache only when reuse justifies it"),
    ("V9", "Output layout", None, None, None, "Optimize final write/layout where required"),
]

columns = [
    "version", "technique", "runtime_seconds",
    "shuffle_read", "shuffle_write", "notes"
]

comparison_df = spark.createDataFrame(results, columns)
display(comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. How to interpret the results
# MAGIC
# MAGIC We are not looking for one magic optimization.
# MAGIC
# MAGIC For each version ask:
# MAGIC
# MAGIC 1. Did the business result remain equivalent?
# MAGIC 2. Did the physical plan change?
# MAGIC 3. Did shuffle decrease?
# MAGIC 4. Did the slowest tasks improve?
# MAGIC 5. Did wall-clock runtime improve?
# MAGIC 6. Did the optimization introduce a new cost?
# MAGIC
# MAGIC A query is not automatically better just because it has fewer partitions or more broadcast joins. Evidence from the Spark UI and executed plan matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Final validation query
# MAGIC
# MAGIC Use this to confirm the final optimized query still produces the required business dimensions.

# COMMAND ----------

final_check = spark.sql("""
SELECT
    p.province,
    h.hospital_name,
    d.speciality,
    dx.category,
    COUNT(*) AS encounter_count
FROM spark_optimization.encounters e
JOIN spark_optimization.patients p
    ON e.patient_id = p.patient_id
JOIN spark_optimization.doctors d
    ON e.doctor_id = d.doctor_id
JOIN spark_optimization.hospitals h
    ON e.hospital_id = h.hospital_id
JOIN spark_optimization.diagnoses dx
    ON e.diagnosis_code = dx.diagnosis_code
WHERE e.encounter_date >= '2025-01-01'
GROUP BY
    p.province,
    h.hospital_name,
    d.speciality,
    dx.category
""")

display(final_check.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Next notebook:** `09_Final_Report`