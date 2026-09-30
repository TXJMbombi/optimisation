# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Final Report
# MAGIC
# MAGIC # Healthcare Spark Optimization Project
# MAGIC
# MAGIC ## Project objective
# MAGIC
# MAGIC Optimize a Spark workload over a deliberately large healthcare encounter dataset without changing the required business result.
# MAGIC
# MAGIC ## Dataset
# MAGIC
# MAGIC - Patients: 2,000,000
# MAGIC - Encounters: 30,000,000
# MAGIC - Doctors: 20,000
# MAGIC - Hospitals: 500
# MAGIC - Diagnoses: 5,000
# MAGIC
# MAGIC ## Business query
# MAGIC
# MAGIC Patient encounters by:
# MAGIC - province
# MAGIC - hospital
# MAGIC - doctor speciality
# MAGIC - diagnosis category
# MAGIC
# MAGIC restricted to encounters from 2025 onwards.
# MAGIC
# MAGIC ## Optimization areas investigated
# MAGIC
# MAGIC 1. Baseline physical plan
# MAGIC 2. Early filtering
# MAGIC 3. Column pruning
# MAGIC 4. Broadcast joins
# MAGIC 5. Shuffle partition tuning
# MAGIC 6. Data skew
# MAGIC 7. Adaptive Query Execution
# MAGIC 8. Controlled caching
# MAGIC 9. Output layout
# MAGIC
# MAGIC ## Evidence
# MAGIC
# MAGIC Paste your measured Spark UI metrics below after completing the experiments.
# MAGIC
# MAGIC | Version | Technique | Runtime | Shuffle Read | Shuffle Write | Observation |
# MAGIC |---|---|---:|---:|---:|---|
# MAGIC | V1 | Baseline | | | | |
# MAGIC | V2 | Filter early | | | | |
# MAGIC | V3 | Column pruning | | | | |
# MAGIC | V4 | Broadcast | | | | |
# MAGIC | V5 | Shuffle partitions | | | | |
# MAGIC | V6 | Skew | | | | |
# MAGIC | V7 | AQE | | | | |
# MAGIC | V8 | Cache | | | | |
# MAGIC | V9 | Output optimization | | | | |
# MAGIC
# MAGIC ## Key lessons
# MAGIC
# MAGIC - Spark transformations are lazy; actions trigger execution.
# MAGIC - Wide transformations commonly introduce shuffles.
# MAGIC - Filtering and column pruning can reduce the amount of data processed.
# MAGIC - Small dimension tables can be candidates for broadcast joins.
# MAGIC - Partition counts should be chosen based on workload and available parallelism, then measured.
# MAGIC - Skew can cause a small number of tasks to become stragglers.
# MAGIC - AQE can adapt parts of the execution plan at runtime.
# MAGIC - Caching is useful when an expensive intermediate is reused enough to justify its storage cost.
# MAGIC - `collect()` and `toPandas()` on a huge dataset can overload the driver.
# MAGIC
# MAGIC ## Final conclusion
# MAGIC
# MAGIC The final optimization should be supported by measured execution evidence rather than assumptions. The goal is not simply to make the code look more sophisticated; it is to reduce unnecessary work while preserving the business result.
