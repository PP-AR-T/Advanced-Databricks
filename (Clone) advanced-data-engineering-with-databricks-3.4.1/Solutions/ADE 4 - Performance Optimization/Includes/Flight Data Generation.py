# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation
# MAGIC
# MAGIC This notebook will set up prerequisite tables and perform the `CLUSTER BY` operation. It is recommended that you use a large cluster with a minimum of 4 workers to run this notebook in a shorter amount of time.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dbacademy

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dbacademy

# COMMAND ----------

df = spark.read.csv('/databricks-datasets/airlines/part-00000', inferSchema=True, header=True)

# COMMAND ----------

df2 = spark.read.csv('/databricks-datasets/airlines/', schema=df.schema, header=True)

# COMMAND ----------

from pyspark.sql.functions import *

(df2
 .withColumn('id', monotonically_increasing_id())
 .select('id', 'year', 'FlightNum', 'ArrDelay', 'UniqueCarrier', 'TailNum')
 .write
 .mode("overwrite")
 .saveAsTable('flights')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE flights ZORDER BY FlightNum

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flights_cluster_id;
# MAGIC CREATE TABLE flights_cluster_id CLUSTER BY (id) AS SELECT * FROM flights

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS flights_cluster_id_flightnum;
# MAGIC CREATE TABLE flights_cluster_id_flightnum
# MAGIC   CLUSTER BY (id, FlightNum)
# MAGIC   AS SELECT * FROM flights

# COMMAND ----------

df_small = spark.read.csv('/databricks-datasets/airlines/part-000*', schema=df.schema, header=True)
df_small.filter('year = 1998 or year = 1999').write.mode("overwrite").saveAsTable('flights_small')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flights

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flights_cluster_id

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flights_cluster_id_flightnum

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
