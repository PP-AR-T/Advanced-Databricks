# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Unit Test Workouts
# MAGIC
# MAGIC In this lab, you will learn to write unit test cases for validation to ensure the quality and accuracy of the data within tables based on specific criteria. You will validate table for quality check and removing duplicate records.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC - How to ensure that the **"workouts_silver"** table contains no null entries and the right data.
# MAGIC - Validating that the **"workouts_completed"** table only contains unique workout IDs.
# MAGIC
# MAGIC **NOTE:** Make sure to run the driver notebook **ADE 5.5L - Develop and Test Pipelines Lab** inside source folder do not execute the cells in this notebook directly
# MAGIC

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test silver table transformations
# MAGIC
# MAGIC To test silver table transformations follow these steps:
# MAGIC 1. Define function with name as **`test_workouts_clean()`** that performs quality checks on a **workouts_silver** table.
# MAGIC 2. Read data from the **workouts_silver** table.
# MAGIC 3. Add a computed column named **workout_id_null** to indicate whether the workout ID is null.
# MAGIC 4. Validate the accuracy of row counts in table.
# MAGIC
# MAGIC

# COMMAND ----------


comment="Test: check silver table removes null workout ids and has correct count"

def test_workouts_clean():
   return (
       FILL_IN
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test primary key uniqueness
# MAGIC
# MAGIC To Test the **workouts_completed** table's primary key uniqueness
# MAGIC - Check if the **duplicate** count is equal to 1 for each unique workout ID in decoraters.
# MAGIC - Define function as **`test_workouts_completed()`** read workout_id and group operation on the data based on the **`workout_id`** column. 
# MAGIC - Count the occurrence of each **`workout_id`**", rename the result as **"duplicate"**.

# COMMAND ----------


comment="Test: check that gold table only contains unique workout id"

def test_workouts_completed():
   return ( 
       FILL_IN
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
