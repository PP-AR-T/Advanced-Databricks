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
# MAGIC # Stream Aggregations Lab
# MAGIC
# MAGIC ### Activity by Traffic
# MAGIC
# MAGIC Process streaming data to display total active users by traffic source.
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Read data stream
# MAGIC 2. Get active users by traffic source
# MAGIC 3. Execute query with display() and plot results
# MAGIC 4. Execute the same streaming query with DataStreamWriter
# MAGIC 5. View results being updated in the query table
# MAGIC 6. List and stop all active streams
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Setup
# MAGIC Run the cells below to generate data and create the **`schema`** string needed for this lab.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.3L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 1. Read data stream
# MAGIC - Set to process 1 file per trigger
# MAGIC - Read from Delta with filepath stored in **`DA.paths.events`**
# MAGIC
# MAGIC Assign the resulting Query to **`df`**.

# COMMAND ----------

df = (spark.readStream
      .format("delta")
      .option("maxFilesPerTrigger", 1)
      .load(DA.paths.events))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_1_1(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Get active users by traffic source
# MAGIC - Set default shuffle partitions to number of cores on your cluster (not required, but runs faster)
# MAGIC - Group by **`traffic_source`**
# MAGIC   - Aggregate the approximate count of distinct users and alias with "active_users"
# MAGIC - Sort by **`traffic_source`**

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct

# Set default shuffle partitions to the number of cores on your cluster
spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)

# Group by traffic_source, aggregate the approximate count of distinct users, alias it with "active_users", and sort by traffic_source
traffic_df = (df.groupBy("traffic_source")
              .agg(approx_count_distinct("user_id").alias("active_users"))
              .orderBy("traffic_source"))

display(traffic_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **2.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_2_1(traffic_df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Execute query with display() and plot results
# MAGIC - Execute results for **`traffic_df`** using display()
# MAGIC - Plot the streaming query results as a bar graph

# COMMAND ----------

display(traffic_df)

# COMMAND ----------

# Import necessary libraries
import matplotlib.pyplot as plt
from pyspark.sql.functions import col

# Write the streaming DataFrame to an in-memory table
query = traffic_df.writeStream \
    .format("memory") \
    .queryName("traffic_table") \
    .outputMode("complete") \
    .start()

# Wait for the stream to initialize
query.awaitTermination(timeout=10)

# Read the data from the in-memory table
static_traffic_df = spark.sql("SELECT * FROM traffic_table")

# Convert the static DataFrame to a Pandas DataFrame
pandas_df = static_traffic_df.toPandas()

# Plot the streaming query results as a bar graph
pandas_df.plot(kind="bar", x="traffic_source", y="active_users")

# Display the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **3.1: CHECK YOUR WORK**
# MAGIC - You bar chart should plot **`traffic_source`** on the x-axis and **`active_users`** on the y-axis
# MAGIC - The top three traffic sources in descending order should be **`google`**, **`facebook`**, and **`instagram`**.

# COMMAND ----------

traffic_query = (traffic_df.writeStream
                 .format("memory")
                 .queryName("active_users_by_traffic")
                 .outputMode("complete")
                 .trigger(processingTime="1 second")
                 .start())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **4.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_4_1(traffic_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM active_users_by_traffic;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **5.1: CHECK YOUR WORK**
# MAGIC Your query should eventually result in the following values.
# MAGIC
# MAGIC |traffic_source|active_users|
# MAGIC |---|---|
# MAGIC |direct|438886|
# MAGIC |email|281525|
# MAGIC |facebook|956769|
# MAGIC |google|1781961|
# MAGIC |instagram|530050|
# MAGIC |youtube|253321|

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. List and stop all active streams
# MAGIC - Use SparkSession to get list of all active streams
# MAGIC - Iterate over the list and stop each query

# COMMAND ----------

# Get the list of all active streaming queries
active_streams = spark.streams.active

# Iterate over the list and stop each query
for query in active_streams:
    query.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **6.1: CHECK YOUR WORK**

# COMMAND ----------

DA.validate_6_1(traffic_query)

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
