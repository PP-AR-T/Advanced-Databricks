# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Modularize Code
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Refactor Python code into reusable functions and relative libraries using arbitrary files in Repos
# MAGIC - Programmatically create tables for a DLT pipeline using Python metaprogramming
# MAGIC - Maintain data quality rules and transformations in portable metadata tables or configuration files
# MAGIC
# MAGIC Generate and trigger an update of a pipeline consisting of the following notebooks:
# MAGIC
# MAGIC | Pipeline |
# MAGIC |---|
# MAGIC | [Bronze / Prod / Ingest]($./Pipeline/bronze/prod/ingest) |
# MAGIC
# MAGIC
# MAGIC Start by running the following setup cell to configure your working environment.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05.2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Segment Ingest and Transform Libraries
# MAGIC
# MAGIC We can modularize our pipeline to easily reuse the same pipeline transformation code on different data sources. 
# MAGIC
# MAGIC The settings below can be used to configure libraries for pipelines that use the same transformation libraries, while using a different ingestion library to replace `<ingest-library>` below.
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "libraries": [
# MAGIC       "notebook": {"path": <ingest-library> },
# MAGIC       "notebook": {"path": ".../transform_silver.py"},
# MAGIC       "notebook": {"path": ".../transform_gold.py"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Say we have three separate libraries that ingest data from different sources into bronze tables for a pipeline.
# MAGIC - `ingest-prod/ingest.py` loads from a kafka stream for production
# MAGIC - `ingest-dev/ingest-subset.py` loads a subset of production data for development
# MAGIC - `ingest-dev/ingest-sample.py` loads sample datasets created for unit testing
# MAGIC
# MAGIC Each of the ingest libraries above define the same tables used as input data in the shared transformation library `transform_silver.py`. For example, all three libraries define a `workouts_bronze` table that is used as input for the `workouts_silver` table in the transformation library.

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

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Land New Data

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Process All Remaining Data

# COMMAND ----------

DA.daily_stream.load(continuous=True)
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
