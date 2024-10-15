# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # PII Lookup Table
# MAGIC
# MAGIC This notebook allows you to programmatically:
# MAGIC
# MAGIC * Generate the DLT pipeline
# MAGIC * Trigger a pipeline run
# MAGIC * Explore the resultant DAG
# MAGIC * Land a new batch of data
# MAGIC
# MAGIC ## The Pipeline
# MAGIC The pipeline we are using in this lesson is located [here]($./Pipeline/ADE 3.1.1 - PII Lookup Table).

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-03.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger a pipeline update.
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the method provided below to land new data. Running this entire notebook again will delete the underlying data files for both the source data and your DLT Pipeline.

# COMMAND ----------

DA.user_reg_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process All Remaining Data
# MAGIC To continuously load all remaining batches of data to the source directory, call the same load method above with the **`continuous`** parameter set to **`True`**.
# MAGIC
# MAGIC Trigger another update to process the remaining data.

# COMMAND ----------

DA.user_reg_stream.load(continuous=True)  # Load all remaining batches of data
DA.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
