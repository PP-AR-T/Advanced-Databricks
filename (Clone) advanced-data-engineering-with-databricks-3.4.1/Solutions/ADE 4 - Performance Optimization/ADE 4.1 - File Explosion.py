# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # File Explosion
# MAGIC We see many data engineers partitioning their tables in ways that can cause major performance issues, without improving future query performance. This is called "over partitioning". We'll see what that looks like in practice in this demo.
# MAGIC
# MAGIC ##### Useful References
# MAGIC - [Partitioning Recomendations](https://docs.databricks.com/en/tables/partitions.html)
# MAGIC - [CREATE TABLE Syntax](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html)
# MAGIC - [About ZORDER](https://docs.databricks.com/en/delta/data-skipping.html)
# MAGIC - [About Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the classroom and disable caching
# MAGIC Run the following cell to set up the lesson.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell, which will set a Spark configuration variable that disables caching. Turning caching off makes the effect of the optimizations more apparent.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Process & Write IoT data
# MAGIC Let's generate some fake IoT data. This first time around, we are only going to generate 2500 rows.

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark
      .range(0, 2500)
      .select(
          hash('id').alias('id'), # randomize our ids a bit
          rand().alias('value'),
          from_unixtime(lit(1701692381 + col('id'))).alias('time') 
      ))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll write the data to a table partitioned by `id`, which will result in every row being written to a separate file. 2500 rows will take a long time to write in this fashion. Note how long it takes to generate the table.

# COMMAND ----------


(df
 .write
 .mode('overwrite')
 .option("overwriteSchema", "true")
 .partitionBy('id')
 .saveAsTable("iot_data")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query the Table
# MAGIC Run the two queries against the table we just wrote. Note the time taken to execute each query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_data where id = 519220707

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(value) FROM iot_data where time >= "2023-12-04 12:19:00" and time <= "2023-12-04 13:01:20"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Note in particular the amount of cloud storage requests and their associated time.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.1-CMD-13-h.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fixing the Problem
# MAGIC
# MAGIC Up to this point, we have been working with 2,500 rows of data. We are now going to increase the volume dramatically by using 50,000,000 rows of data. If we had tried the code above with a dataset this large, it would take considerably longer.
# MAGIC
# MAGIC As before, the following cell generates the data.

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark
      .range(0,50000000, 1, 32) 
      .select(
          hash('id').alias('id'), # randomize our ids a bit
          rand().alias('value'),
          from_unixtime(lit(1701692381 + col('id'))).alias('time') 
      ))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now we'll establish a table to capture the data, this time without partitioning. Doing it this way accomplishes the following:
# MAGIC - take less time to run, even on larger data sets
# MAGIC - writes fewer files
# MAGIC - writes faster
# MAGIC - selects for one id in about the same time
# MAGIC - filters by time faster

# COMMAND ----------

(df
 .write
 .option("overwriteSchema", "true")
 .mode('overwrite')
 .saveAsTable("iot_data")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate optimization
# MAGIC The next two cells repeat the queries from earlier and will put this change to the test. The first cell should run almost as fast as before, and the second cell should run much faster.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_data where id = 519220707

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(value) FROM iot_data where time >= "2023-12-04 12:19:00" and time <= "2023-12-04 13:01:20"

# COMMAND ----------

# MAGIC %md
# MAGIC Let's see how this query performed using the Spark UI. Compare the results against the same query we performed earlier against an over-partitioned table.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.1-CMD-20-h.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liquid Clustering
# MAGIC An alternative to partitioning is [Liquid Clustering](https://docs.databricks.com/en/delta/clustering.html). Liquid clustering performs much better than partitioning, especially for high cardinality columns. We will look at Liquid Clustering in the next demo.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
