# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Develop and Test Pipelines Lab
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Prepare sample datasets for development environments
# MAGIC - Define unit tests for ingestion and transformation steps in a DLT pipeline
# MAGIC - Use temporary DLT tables with expectations to implement unit tests
# MAGIC
# MAGIC Generate and trigger an update of a pipeline consisting of the following notebooks:
# MAGIC
# MAGIC | Pipeline |
# MAGIC |---|
# MAGIC | Bronze / Dev / Ingest Test |
# MAGIC | Silver / Quarantine |
# MAGIC | Silver / Users |
# MAGIC | Silver / Workouts BPM |
# MAGIC | Tests / Users Test |
# MAGIC | [Tests / Workouts Test]($./Pipeline/lab/tests/workouts_test) |
# MAGIC
# MAGIC Start by running the following setup cell to configure your working environment.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05.6L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values. Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Trigger Pipeline Run
# MAGIC Use the method provided below to trigger a pipeline update.
# MAGIC
# MAGIC **IMPORTANT:** You'll need to complete the lab (TODO) in the DLT notebook [workouts_test]($./Pipeline/lab/tests/workouts_test) for this pipeline to run successfully.
# MAGIC

# COMMAND ----------

# TODO: Make sure to complete the lab in the DLT notebook linked above (workouts_test) before running this pipeline
DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
