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
# MAGIC # User-Defined Functions
# MAGIC
# MAGIC Databricks recommends using native functions whenever possible. While UDFs are a great way to extend the functionality of Spark SQL, their use requires transferring data between Python and Spark, which in turn requires serialization. This drastically slows down queries.
# MAGIC
# MAGIC But sometimes UDFs are necessary. They can be an especially powerful tool for ML or NLP use cases, which may not have a native Spark equivalent.
# MAGIC
# MAGIC Run the next cell to set up the lesson.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.4

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell, which will set a Spark configuration variable that disables caching. Turning caching off makes the effect of the optimizations more apparent.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate Data
# MAGIC
# MAGIC Let's generate the data we will use in this demo. For this, we'll synthesize telemetry data representing temperature readings.  This time, however, we're only going to generate 60 readings.

# COMMAND ----------

from pyspark.sql.functions import *

df = (spark
      .range(0, 60, 1, 1)
      .select(
          'id',
          (col('id') % 1000).alias('device_id'),
          (rand() * 100).alias('temperature_F')
      )
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll write the data to a table.

# COMMAND ----------

df.write.saveAsTable('device_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Computationally Expensive UDF
# MAGIC
# MAGIC For the sake of experimentation, let's implement a function that converts Fahrenheit to Celsius. Notice that we're inserting a one-second sleep to simulate a computationally expensive operation within our UDF. Let's try it.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

@udf("double")
def F_to_Celsius(f):
    # Let's pretend some fancy math takes one second per row
    time.sleep(1)
    return (f - 32) * (5/9)

celsius_df = (spark.table('device_data')
                .withColumn("celsius", F_to_Celsius(col('temperature_F')))
             )

celsius_df.write.mode('overwrite').saveAsTable('celsius')

# COMMAND ----------

# MAGIC %md
# MAGIC That took approximately one minute, which is kind of surprising since we have about 60 seconds worth of computation, spread across multiple cores. Shouldn't it take significantly less time? 
# MAGIC
# MAGIC The answer to this question is yes, it should take less time. The problem here is that Spark doesn't know that the computations are expensive, so it hasn't divided the work up into tasks that can be done in parallel. We can see that by watching the one task chug away as the cell is running, and by visiting the Spark UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parallelization through Repartitioning
# MAGIC
# MAGIC Repartitioning is the answer in this case. *We* know that this computation is expensive and should span across all 4 cores, so we can explicitly repartition the dataframe:

# COMMAND ----------

# Repartition across the number of cores in your cluster
num_cores = 4

@udf("double")
def F_to_Celsius(f):
    # Let's pretend some fancy math take one second per row
    time.sleep(1)
    return (f - 32) * (5/9)

celsius_df = (spark.table('device_data')
                .repartition(num_cores) # <-- HERE
                .withColumn("celsius", F_to_Celsius(col('temperature_F')))
             )

celsius_df.write.mode('overwrite').saveAsTable('celsius')

# COMMAND ----------

celsius_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # SQL UDFs
# MAGIC
# MAGIC The ability to create user-defined functions in Python and Scala is convenient since it allows you to extend functionality in the language of your choice. As far as optimization is concerned, however, it's important to know that SQL is generally the best choice, for a couple of reasons:
# MAGIC - SQL UDFs require less data serialization
# MAGIC - Catalyst optimizer can operate within SQL UDFs
# MAGIC
# MAGIC Let's see this in action now by comparing the performance of a SQL UDF to its Python counterpart.
# MAGIC
# MAGIC First let's redefine the Python UDF from before, this time without the delay, so we can compare raw performance.

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's perform the equivalent operation through a SQL UDF.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION IF EXISTS farh_to_cels;
# MAGIC CREATE FUNCTION farh_to_cels (farh DOUBLE)
# MAGIC   RETURNS DOUBLE RETURN ((farh - 32) * 5/9);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE celsius_sql AS
# MAGIC   SELECT farh_to_cels(temperature_F) as Farh_to_cels_convert FROM device_data;

# COMMAND ----------

# MAGIC %sql
# MAGIC explain SELECT farh_to_cels(temperature_F) as Farh_to_cels_convert FROM device_data

# COMMAND ----------

# MAGIC %md
# MAGIC Actual times depend on a number of factors, however on average the SQL UDF will perform better than its Python equivalent - often, significantly better.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
