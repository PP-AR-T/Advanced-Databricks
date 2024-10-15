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
# MAGIC
# MAGIC # Propagating Changes with CDF Lab
# MAGIC
# MAGIC We'll be using Change Data Feed to propagate changes to many tables from a single source.
# MAGIC
# MAGIC For this lab, we'll work with the fitness tracker datasets to propagate changes through a Lakehouse with Delta Lake Change Data Feed (CDF).
# MAGIC
# MAGIC Because the **`user_lookup`** table links identifying information between different pipelines, we'll make this the point where changes propagate from.
# MAGIC
# MAGIC
# MAGIC ## Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC - Enable Change Data Feed on a particular table
# MAGIC - Read CDF output with Spark SQL or PySpark
# MAGIC - Refactor ELT code to process CDF output

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC Begin by running the following cell to set up relevant databases and paths.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03.5L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enables CDF for the table
# MAGIC
# MAGIC To enables CDF for the **"user_lookup"** table use ALTER TABLE and set TBLPROPERTIES to activate **`delta.enableChangeDataFeed`**.

# COMMAND ----------

# TODO
# <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Read the CDF output from table
# MAGIC
# MAGIC To read the CDF data:
# MAGIC - Set up a streaming read on the **`user_lookup`** table
# MAGIC - Configure the stream to enable reading change data
# MAGIC - Configure the stream to start reading from version 1 of the **`user_lookup`** table

# COMMAND ----------


user_lookup_df = (FILL_IN)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete a record from table
# MAGIC
# MAGIC To delete record from table:
# MAGIC - Use **`DELETE`** statement with column_name of table
# MAGIC - Enter actual name of the column and value to delete record

# COMMAND ----------

# TODO
# <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check that the record was deleted from user_lookup
# MAGIC
# MAGIC To check whether record was deleted:
# MAGIC - Use **`SELECT`** statement to get record from table
# MAGIC - Specify the column_name and value to see whether record with specific id exist in table

# COMMAND ----------

# TODO
# <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Propagate deletes from multiple tables
# MAGIC
# MAGIC To propagate delete follow these steps:
# MAGIC - Create temporary view of user_lookup table as **`user_lookup_deletes`**
# MAGIC - Select all record in view where **`_change_type`** is **delete**  
# MAGIC - Merge into **`users`** table when **`alt_id`** gets matched
# MAGIC - Similarly, merge into **`user_bins`** table when **`user_id`** gets matched

# COMMAND ----------


# CREATE OR REPLACE TEMPORARY VIEW FILL_IN 

# MERGE INTO users u
# USING FILL_IN

# MERGE INTO user_bins ub
# USING FILL_IN

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
