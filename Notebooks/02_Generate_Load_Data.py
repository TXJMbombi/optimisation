# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — Generate and Load Data
# MAGIC
# MAGIC **Purpose:** Generate the five datasets from the original project and write them as Delta tables.
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `patients` — 2M rows
# MAGIC - `hospitals` — 500 rows
# MAGIC - `doctors` — 20K rows
# MAGIC - `diagnoses` — 5K rows
# MAGIC - `encounters` — 30M rows
# MAGIC
# MAGIC The `encounters` dataset intentionally contains strong `hospital_id` skew so that later notebooks can demonstrate shuffle and skew optimization.
# MAGIC
# MAGIC **Run this notebook only after `01_Setup_Environment`.**

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE spark_optimization")

# Original project configuration
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Patients
# MAGIC
# MAGIC Approximately 2 million patients.

# COMMAND ----------

patients = (
    spark.range(0, NUM_PATIENTS, 1, PATIENT_PARTITIONS)
    .withColumnRenamed("id", "patient_id")
    .withColumn(
        "gender",
        F.when(F.pmod(F.col("patient_id"), 2) == 0, "Male").otherwise("Female")
    )
    .withColumn(
        "province",
        F.element_at(
            F.array(
                F.lit("Gauteng"), F.lit("KwaZulu-Natal"), F.lit("Western Cape"),
                F.lit("Eastern Cape"), F.lit("Limpopo"), F.lit("Mpumalanga"),
                F.lit("North West"), F.lit("Free State"), F.lit("Northern Cape")
            ),
            (F.pmod(F.col("patient_id"), 9).cast('int') + 1)
        )
    )
    .withColumn(
        "date_of_birth",
        F.date_sub(
            F.current_date(),
            (F.pmod(F.col("patient_id"), 25_000) + 7_000).cast("int")
        )
    )
    .withColumn(
        "registration_date",
        F.date_sub(
            F.current_date(),
            F.pmod(F.col("patient_id"), 3_000).cast("int")
        )
    )
)

patients.printSchema()
patients.show(10, truncate=False)

# COMMAND ----------

patients.write.format("delta").mode("overwrite").saveAsTable("patients")
print("patients written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Hospitals
# MAGIC
# MAGIC This is deliberately small so we can demonstrate broadcast joins later.

# COMMAND ----------

hospitals = (
    spark.range(1, NUM_HOSPITALS + 1, 1, HOSPITAL_PARTITIONS)
    .withColumnRenamed("id", "hospital_id")
    .withColumn("hospital_name", F.concat(F.lit("Hospital_"), F.col("hospital_id")))
    .withColumn(
        "province",
        F.element_at(
            F.array(
                F.lit("Gauteng"), F.lit("KwaZulu-Natal"), F.lit("Western Cape"),
                F.lit("Eastern Cape"), F.lit("Limpopo"), F.lit("Mpumalanga"),
                F.lit("North West"), F.lit("Free State"), F.lit("Northern Cape")
            ),
            (F.pmod(F.col("hospital_id"), 9) + 1).cast("int")
        )
    )
    .withColumn(
        "hospital_type",
        F.element_at(
            F.array(F.lit("Public"), F.lit("Private"), F.lit("Academic")),
            F.pmod(F.col("hospital_id"), 3).cast("int") + 1
        )
    )
)

hospitals.write.format("delta").mode("overwrite").saveAsTable("hospitals")
print("hospitals written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Doctors

# COMMAND ----------

specialities = [
    "General Practitioner", "Cardiology", "Oncology", "Neurology",
    "Paediatrics", "Orthopaedics", "Dermatology", "Psychiatry",
    "Radiology", "Emergency Medicine"
]

speciality_expr = F.element_at(
    F.array(*[F.lit(x) for x in specialities]),
    F.pmod(F.col("doctor_id"), F.lit(len(specialities))).cast('int') + 1
)

doctors = (
    spark.range(1, NUM_DOCTORS + 1, 1, DOCTOR_PARTITIONS)
    .withColumnRenamed("id", "doctor_id")
    .withColumn("doctor_name", F.concat(F.lit("Doctor_"), F.col("doctor_id")))
    .withColumn("speciality", speciality_expr)
    .withColumn(
        "hospital_id",
        F.pmod(F.col("doctor_id"), F.lit(NUM_HOSPITALS)) + 1
    )
)

doctors.write.format("delta").mode("overwrite").saveAsTable("doctors")
print("doctors written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Diagnoses

# COMMAND ----------

diagnosis_categories = [
    "Cardiovascular", "Respiratory", "Neurological", "Infectious",
    "Oncology", "Orthopaedic", "Dermatological", "Gastrointestinal",
    "Endocrine", "General"
]

diagnoses = (
    spark.range(1, NUM_DIAGNOSES + 1, 1, DIAGNOSIS_PARTITIONS)
    .withColumnRenamed("id", "diagnosis_id")
    .withColumn(
        "diagnosis_code",
        F.concat(
            F.lit("DX"),
            F.lpad(F.col("diagnosis_id").cast("string"), 5, "0")
        )
    )
    .withColumn(
        "diagnosis_description",
        F.concat(
            F.lit("Diagnosis_"),
            F.col("diagnosis_id")
        )
    )
    .withColumn(
        "category",
        F.element_at(
            F.array(*[F.lit(x) for x in diagnosis_categories]),
            (F.pmod(F.col("diagnosis_id"), 10) + 1).cast("int")
        )
    )
)

diagnoses.write.format("delta").mode("overwrite").saveAsTable("diagnoses")

print("diagnoses written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Encounters — deliberately skewed
# MAGIC
# MAGIC Hospital `1` receives approximately 55% of all encounters.
# MAGIC
# MAGIC This is intentional and will be used later to study:
# MAGIC - shuffle
# MAGIC - partition imbalance
# MAGIC - join skew
# MAGIC - AQE
# MAGIC - salting/skew-handling concepts

# COMMAND ----------

encounters = (
    spark.range(0, NUM_ENCOUNTERS, 1, ENCOUNTER_PARTITIONS)
    .withColumnRenamed("id", "encounter_id")

    .withColumn(
        "patient_id",
        F.pmod(
            F.col("encounter_id") * 17,
            F.lit(NUM_PATIENTS)
        )
    )

    .withColumn(
        "encounter_date",
        F.date_sub(
            F.current_date(),
            F.pmod(
                F.col("encounter_id"),
                1_000
            ).cast("int")
        )
    )

    .withColumn(
        "encounter_type",
        F.element_at(
            F.array(
                F.lit("Outpatient"),
                F.lit("Inpatient"),
                F.lit("Emergency"),
                F.lit("Follow-up")
            ),
            (F.pmod(F.col("encounter_id"), 4) + 1).cast("int")
        )
    )

    .withColumn(
        "doctor_id",
        F.pmod(
            F.col("encounter_id") * 13,
            F.lit(NUM_DOCTORS)
        ) + 1
    )

    .withColumn(
        "hospital_id",
        F.when(
            F.pmod(F.col("encounter_id"), 100) < 55,
            F.lit(1)
        ).otherwise(
            F.pmod(
                F.col("encounter_id"),
                F.lit(NUM_HOSPITALS - 1)
            ) + 2
        )
    )

    .withColumn(
        "diagnosis_code",
        F.concat(
            F.lit("DX"),
            F.lpad(
                (
                    F.pmod(
                        F.col("encounter_id") * 7,
                        F.lit(NUM_DIAGNOSES)
                    ) + 1
                ).cast("string"),
                5,
                "0"
            )
        )
    )

    .withColumn(
        "status",
        F.element_at(
            F.array(
                F.lit("Completed"),
                F.lit("Cancelled"),
                F.lit("In Progress")
            ),
            (F.pmod(F.col("encounter_id"), 3) + 1).cast("int")
        )
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify skew before writing
# MAGIC
# MAGIC This action scans the generated DataFrame. It is intentionally performed once here because skew is a core part of the project.

# COMMAND ----------

encounters.groupBy("hospital_id") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write encounters

# COMMAND ----------

encounters.write.format("delta").mode("overwrite").saveAsTable("encounters")
print("encounters written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data generation complete
# MAGIC
# MAGIC **Next notebook:** `03_Validate_Data`