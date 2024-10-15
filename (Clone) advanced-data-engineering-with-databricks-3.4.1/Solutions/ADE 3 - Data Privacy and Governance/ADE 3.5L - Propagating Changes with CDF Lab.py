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

# MAGIC %sql
# MAGIC ALTER TABLE user_lookup 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

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

user_lookup_df = (spark.readStream
           .format("delta")
           .option("readChangeData", True)
           .option("startingVersion", 1)
           .table("user_lookup"))

display(user_lookup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete a record from table
# MAGIC
# MAGIC To delete record from table:
# MAGIC - Use **`DELETE`** statement with column_name of table
# MAGIC - Enter actual name of the column and value to delete record

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM user_lookup WHERE user_id = 49661

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check that the record was deleted from user_lookup
# MAGIC
# MAGIC To check whether record was deleted:
# MAGIC - Use **`SELECT`** statement to get record from table
# MAGIC - Specify the column_name and value to see whether record with specific id exist in table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM user_lookup WHERE user_id = 49661

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

# MAGIC %sql
# MAGIC
# MAGIC -- Create a temporary view for change data with delete entries
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW user_lookup_deletes AS
# MAGIC SELECT *
# MAGIC FROM table_changes("user_lookup", 1)
# MAGIC WHERE _change_type = 'delete';
# MAGIC
# MAGIC -- Apply deletions to the "user" and "user_bin" tables
# MAGIC MERGE INTO users u
# MAGIC USING user_lookup_deletes uld
# MAGIC ON u.alt_id = uld.alt_id
# MAGIC WHEN MATCHED
# MAGIC  THEN DELETE;
# MAGIC
# MAGIC MERGE INTO user_bins ub
# MAGIC USING user_lookup_deletes uld
# MAGIC ON ub.user_id = uld.user_id
# MAGIC WHEN MATCHED
# MAGIC  THEN DELETE;
# MAGIC

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
