# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading from a Streaming Query
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Build streaming DataFrames
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC 1. Monitor a streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.1

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Build streaming DataFrames
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

DA.paths.events

# COMMAND ----------

df = (spark
      .readStream
      .format("delta")
      .load(DA.paths.events)
)

print("Is the dataframe streaming:", df.isStreaming)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Apply some transformations, producing new streaming DataFrames.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count
# import F. Also, cell imports in 
email_traffic_df = (df
                    .filter(col("traffic_source") == "email")
                    .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                    .select("user_id", "event_timestamp", "mobile")
                   )

email_traffic_df.isStreaming

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write streaming query results
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a file sink in "append" mode.

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/email_traffic"
output_path = f"{DA.paths.working_dir}/email_traffic/output"

devices_query = (email_traffic_df
                 .writeStream
                 .outputMode("append")
                 .format("delta") # Although default is Delta, we're explicitly calling this out. This line is vestigial as of DBR 8.0
                 .queryName("email_traffic")
                 .trigger(processingTime="1 second")
                 .option("checkpointLocation", checkpoint_path)
                 .start(output_path)
                )

# COMMAND ----------

DA.block_until_stream_is_ready(devices_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monitor streaming query
# MAGIC
# MAGIC Use the streaming query handle to monitor and control it.

# COMMAND ----------

devices_query.id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Query status output

# COMMAND ----------

devices_query.status

# COMMAND ----------

# MAGIC %md
# MAGIC [lastProgress](https://spark.apache.org/docs/3.5.2/structured-streaming-programming-guide.html#managing-streaming-queries) gives us metrics from the previous query

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC [awaitTermination](https://spark.apache.org/docs/3.5.2/structured-streaming-programming-guide.html#managing-streaming-queries) blocks the current thread until the streaming query is terminated. In our course notebooks we use it in case you use "Run All" to run the notebook to prevent subsequent command cells from executing until the streaming query has fully terminated.
# MAGIC
# MAGIC For stand-alone structured streaming applications, this is used to prevent the main thread from terminating while the streaming query is still executing.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

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
