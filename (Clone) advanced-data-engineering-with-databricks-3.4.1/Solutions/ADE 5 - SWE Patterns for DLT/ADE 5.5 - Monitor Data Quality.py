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
# MAGIC # Monitor Data Quality
# MAGIC
# MAGIC Each DLT Pipeline saves events and expectations metrics in the Storage Location defined on the pipeline. From this table we can see what is happening and the quality of the data passing through it.
# MAGIC
# MAGIC You can leverage the expectations directly as a SQL table with Databricks SQL to track your expectation metrics and send alerts as required. 
# MAGIC
# MAGIC This notebook extracts and analyses expectation metrics to build such KPIS.
# MAGIC
# MAGIC You can find your metrics opening the Settings of your DLT pipeline, under `storage`

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-05.5

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## System table setup
# MAGIC We'll create a table based on the events log being saved by DLT. The system tables are stored under the storage location defined in your DLT pipeline settings.

# COMMAND ----------

display(dbutils.fs.ls(DA.paths.pipeline_event_logs))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DLT expectation analysis
# MAGIC Delta live table tracks our data quality through expectations. These expectations are stored as technical tables without the DLT log events. We can create a view to simply analyze this information

# COMMAND ----------

import pyspark.sql.functions as F

( 
    spark.read.load(DA.paths.pipeline_event_logs)
        .sort(F.col("timestamp").desc())
        .createOrReplaceTempView("pipeline_event_logs")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pipeline_event_logs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event logs table structure
# MAGIC The `details` column contains metadata about each Event sent to the Event Log. There are different fields depending on what type of Event it is. Some examples include:
# MAGIC * `user_action` Events occur when taking actions like creating the pipeline
# MAGIC * `flow_definition` Events occur when a pipeline is deployed or updated and have lineage, schema, and execution plan information
# MAGIC   * `output_dataset` and `input_datasets` - output table/view and its upstream table(s)/view(s)
# MAGIC   * `flow_type` - whether this is a complete or append flow
# MAGIC   * `explain_text` - the Spark explain plan
# MAGIC * `flow_progress` Events occur when a data flow starts running or finishes processing a batch of data
# MAGIC   * `metrics` - currently contains `num_output_rows`
# MAGIC   * `data_quality` - contains an array of the results of the data quality rules for this particular dataset
# MAGIC     * `dropped_records`
# MAGIC     * `expectations`
# MAGIC       * `name`, `dataset`, `passed_records`, `failed_records`
# MAGIC       
# MAGIC We can leverage this information to track our table quality using SQL

# COMMAND ----------

# DBTITLE 0,Event Log - Raw Sequence of Events by Timestamp
# MAGIC %sql
# MAGIC SELECT
# MAGIC   id,
# MAGIC   timestamp,
# MAGIC   sequence,
# MAGIC   event_type,
# MAGIC   message,
# MAGIC   level,
# MAGIC   details
# MAGIC FROM
# MAGIC   pipeline_event_logs
# MAGIC ORDER BY
# MAGIC   timestamp ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW data_quality_metrics AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows AS output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status AS status_update,
# MAGIC     explode(
# MAGIC       from_json(
# MAGIC         details:flow_progress.data_quality.expectations,
# MAGIC         'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>'
# MAGIC       )
# MAGIC     ) AS expectations
# MAGIC   FROM pipeline_event_logs
# MAGIC   where details:flow_progress.data_quality.expectations IS NOT null
# MAGIC   ORDER BY timestamp);
# MAGIC   
# MAGIC select * from data_quality_metrics

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Visualizing the Quality Metrics
# MAGIC
# MAGIC Let's run a few queries to show the metrics we can display. Ideally, we should be using Databricks SQL to create SQL Dashboard and track all the data, but for this example we'll run a quick query in the dashboard directly:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     sum(expectations.failed_records) AS failed_records, 
# MAGIC     sum(expectations.passed_records) AS passed_records, 
# MAGIC     expectations.name 
# MAGIC FROM data_quality_metrics 
# MAGIC GROUP BY expectations.name

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plotting failed record per expectations
# MAGIC
# MAGIC * This visualization represents the data quality metrics with the number of passed and failed records for different expectations.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   expectations.name,
# MAGIC   SUM(expectations.passed_records) AS passed_records,
# MAGIC   SUM(expectations.failed_records) AS failed_records
# MAGIC FROM data_quality_metrics
# MAGIC GROUP BY expectations.name
# MAGIC -- Optionally, you can order the results by a specific column
# MAGIC -- ORDER BY passed_records DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### What's next?
# MAGIC
# MAGIC We now have our data ready to be used for more advanced.
# MAGIC
# MAGIC We can start creating our first DBSQL Dashboard monitoring our data quality & DLT pipeline health.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
