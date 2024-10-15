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
# MAGIC # Data Skipping
# MAGIC In this demo, we are going to work with Liquid Clustering, a Delta Lake optimization feature that replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. It provides flexibility to redefine clustering keys without rewriting data. Refer to the [documentation](https://docs.databricks.com/en/delta/clustering.html) for more information.
# MAGIC
# MAGIC **PLEASE NOTE:** this demo relies on a specific set of tables. If this course is being led by an instructor, these tables will have been already set up to save time. The datasets that demonstrate Liquid Clustering are large, and the clusters we use with demos and labs are small.
# MAGIC
# MAGIC If you are taking this course as a self-paced course on your own, you will need to run the [Flight Data Generation notebook]($./Includes/Flight Data Generation) first.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Started
# MAGIC Run the next cell to set up the lesson.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.2

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell, which will set a Spark configuration variable that disables caching. Turning caching off makes the effect of the optimizations more apparent.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's see a count of the number of records in the `flights` table.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM dbacademy.flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM dbacademy.flights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Liquid Clustering
# MAGIC Delta Lake liquid clustering replaces table partitioning and ZORDER to simplify data layout decisions and optimize query performance. Liquid clustering provides flexibility to redefine clustering keys without rewriting existing data, allowing data layout to evolve alongside analytic needs over time.  
# MAGIC   
# MAGIC Databricks recommends liquid clustering for all new Delta tables. The following are examples of scenarios that benefit from clustering:
# MAGIC - Tables often filtered by high cardinality columns.
# MAGIC - Tables with significant skew in data distribution.
# MAGIC - Tables that grow quickly and require maintenance and tuning effort.
# MAGIC - Tables with concurrent write requirements.
# MAGIC - Tables with access patterns that change over time.
# MAGIC - Tables where a typical partition key could leave the table with too many or too few partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## About the Following Tables
# MAGIC
# MAGIC We will be querying three tables: 
# MAGIC * `flights`, which does not use liquid clustering
# MAGIC * `flights_cluster_id`, which has been clustered by `id`
# MAGIC * `flights_cluster_id_flight_num`, which has been clustered by `id` and `FlightNum`. 
# MAGIC
# MAGIC Before proceeding, let's remind ourselves what the data looks like.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbacademy.flights

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unclustered Table
# MAGIC Run the following three cells and note the time it takes to run each cell.  
# MAGIC   
# MAGIC These queries are already quite fast without using clustering, considering we are using a small cluster, and the tables are 9 GB of data. But there is room for improvement.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights WHERE UniqueCarrier = 'TW'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE HISTORY dbacademy.flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights WHERE FlightNum = 1890

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbacademy.flights WHERE id = 1125281431554

# COMMAND ----------

# MAGIC %md
# MAGIC For all three queries above, drop open the triangle next to **Spark Jobs** and click **View** next to the first job. Drop open **Completed Stages** and note the amount of data that had to be pulled from the data store (in the **Input** column).
# MAGIC
# MAGIC Open the Spark UI to inspect query performance.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.2-CMD-17-h.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clustered by ID
# MAGIC Run the following three queries. These queries pull data from the `flights_cluster_id` table. This table is exactly the same as the one we used in the queries above, except that we have enabled liquid clustering by adding `CLUSTER BY (id)` when the table was created.  
# MAGIC   
# MAGIC Note the following:
# MAGIC - When we query by the clustered column (id), we see a significant improvement in query performance
# MAGIC - We don't see a degradation in performance on queries against unclustered columns  
# MAGIC
# MAGIC View the first job in the query that filters by `id` and note the amount of data that was skipped. You can also see in the query plan that the filter was pushed down.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights_cluster_id WHERE UniqueCarrier = 'TW'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights_cluster_id WHERE FlightNum = 1890

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbacademy.flights_cluster_id WHERE id = 1125281431554

# COMMAND ----------

# MAGIC %md
# MAGIC Open the Spark UI to inspect query performance. In particular, note the cloud storage requests and associated time.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.2-CMD-22-1-h.png)
# MAGIC
# MAGIC Also, notice the sizes of the processed files.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.2-CMD-22-2-h.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clustered by ID and FlightNum
# MAGIC
# MAGIC Run the following three queries. These queries pull data from the `flights_cluster_id_flightnum` table. This table is clustered by both the `id` and `flight_num` columns.  
# MAGIC
# MAGIC Note the following:
# MAGIC - We still don't have any degradation on unclustered columns. Had we used `PARTITION BY` to partition by `flight_num` and `id`, we would see massive slowdown for any queries not on those columns, and writes would be prohibitively slow for this volume of data
# MAGIC - Now queries on flight number are improved
# MAGIC - Queries are a little slower on id now, however and we can look at the DAG to see why.
# MAGIC
# MAGIC Note that, because  we had to read more files to satisfy this request. There is a (small) cost to clustering on more columns, so choose wisely.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights_cluster_id_flightnum WHERE UniqueCarrier = 'TW'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(ArrDelay) FROM dbacademy.flights_cluster_id_flightnum WHERE FlightNum = 1890

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dbacademy.flights_cluster_id_flightnum WHERE id = 1125281431554

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
