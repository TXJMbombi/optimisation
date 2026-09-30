# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — Setup Environment
# MAGIC
# MAGIC **Purpose:** Create the project schema and establish the common Spark project settings.
# MAGIC
# MAGIC This notebook does **not** generate the 30M-row dataset. Run it first.
# MAGIC
# MAGIC **Project:** Healthcare Spark Optimization
# MAGIC
# MAGIC **Important:** We are using Serverless compute. Keep Photon disabled initially if the UI allows it, so the optimization experiments are easier to interpret.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the project database/schema
# MAGIC
# MAGIC All project tables will live under `spark_optimization`.

# COMMAND ----------

spark.sql("""
CREATE DATABASE IF NOT EXISTS spark_optimization
""")

spark.sql("USE spark_optimization")

print("Using database/schema: spark_optimization")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify the active database

# COMMAND ----------

spark.sql("SELECT current_database() AS current_database").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Project configuration
# MAGIC
# MAGIC These are the original project volumes. We keep them centralized here for documentation.
# MAGIC
# MAGIC The actual data-generation notebook repeats these constants because Python variables do not automatically persist between separate Databricks notebooks.

# COMMAND ----------

NUM_PATIENTS = 2_000_000
NUM_ENCOUNTERS = 30_000_000
NUM_DOCTORS = 20_000
NUM_HOSPITALS = 500
NUM_DIAGNOSES = 5_000

PATIENT_PARTITIONS = 100
ENCOUNTER_PARTITIONS = 300
DOCTOR_PARTITIONS = 20
HOSPITAL_PARTITIONS = 10
DIAGNOSIS_PARTITIONS = 20

print("Project configuration loaded/documented.")
print(f"Patients:    {NUM_PATIENTS:,}")
print(f"Encounters:  {NUM_ENCOUNTERS:,}")
print(f"Doctors:     {NUM_DOCTORS:,}")
print(f"Hospitals:   {NUM_HOSPITALS:,}")
print(f"Diagnoses:   {NUM_DIAGNOSES:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect current Spark settings
# MAGIC
# MAGIC We record the relevant settings without changing them yet. Later notebooks deliberately change specific settings as part of the experiments.

# COMMAND ----------

print("Spark version:", spark.version)
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
try:    print("AQE enabled:", spark.conf.get("spark.sql.adaptive.enabled"))
except Exception:
    print("AQE enabled: Configuration not available")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete
# MAGIC
# MAGIC **Next notebook:** `02_Generate_Load_Data`