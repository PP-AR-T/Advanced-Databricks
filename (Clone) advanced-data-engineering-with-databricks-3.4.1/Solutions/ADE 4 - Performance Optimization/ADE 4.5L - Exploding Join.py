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
# MAGIC # Exploding Join
# MAGIC
# MAGIC In this lab we will be working on Exploding join between 3 tables, Transactions, Stores and Countries to identify Spill induced by exploding join, and gradully improve performance of application.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.5L

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Disable cache to avoid side effect of cache.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Process & Write Transaction data
# MAGIC - Generate Transaction data with schema below and write to Delta table. This is the table with largest amount of data.

# COMMAND ----------

from pyspark.sql.functions import *

transactions_df = (spark
                        .range(0, 2000000, 1, 32)
                        .select(
                            'id',
                            round(rand() * 10000, 2).alias('amount'),
                            (col('id') % 10).alias('country_id'),
                            (col('id') % 100).alias('store_id')
                        )
                    )

transactions_df.display()

# COMMAND ----------

transactions_df.write.mode('overwrite').option("overwriteSchema", "true").saveAsTable('transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Process & Write Countries data
# MAGIC - Generate Countries data to join with Transaction on later 
# MAGIC - Write countries data to Delta table

# COMMAND ----------

countries = [(0, "Italy"),
             (1, "Canada"),
             (2, "Mexico"),
             (3, "China"),
             (4, "Germany"),
             (5, "UK"),
             (6, "Japan"),
             (7, "Korea"),
             (8, "Australia"),
             (9, "France"),
             (10, "Spain"),
             (11, "USA")
            ]

columns = ["id", "name"]
countries_df = spark.createDataFrame(data = countries, schema = columns)

countries_df.display()

# COMMAND ----------

countries_df.write.mode('overwrite').saveAsTable("countries")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3. Process & Write Store data
# MAGIC - Generate Stores data to join with Transaction on later 
# MAGIC - Stores data intentionally put duplicates of ids which will induce explode join with transactions. When join Transactions with Stores data, number of rows will explode because of those duplicated store_id. 
# MAGIC - Write Stores data to Delta table

# COMMAND ----------

stores_df = (spark
                .range(0, 9999)
                .select(
                    (col('id') % 100).alias('id'), # intentionally duplicating ids to explode the join
                    round(rand() * 100, 0).alias('employees'),
                    (col('id') % 10).alias('country_id'),
                    expr('uuid()').alias('name')
                )
            )

stores_df.display()

# COMMAND ----------

stores_df.write.mode('overwrite').saveAsTable('stores')

# COMMAND ----------

# MAGIC %md
# MAGIC We will be joining transctions with countries and stores data and trigger action by save to table. 
# MAGIC

# COMMAND ----------


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", -1)



joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO DO 1: 
# MAGIC Note down amount of time taken and identify below information in Spark UI. 
# MAGIC - See the spill in the Spark UI. hint: find the stage with largest amount of shuffle writes. 
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-18-spill-h.png)
# MAGIC - Identify the explosion of rows in the DAG of SQL/Dataframe section of Spark UI.
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-18-explode-1-h.png)
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-18-explode-2-h.png)
# MAGIC - Note down the time taken to do.

# COMMAND ----------

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### TO DO 2: Can we do better? 
# MAGIC Other than remove duplicates in stores_id or change to broadcast hash join?

# COMMAND ----------

# manually set shuffle partitions, how many tasks for last stage of last job? 
spark.conf.set("spark.sql.shuffle.partitions", 8)

joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Note down amount of data spilled has been reduced and time taken to execute action.

# COMMAND ----------

# MAGIC %md
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-21-spill-h.png)

# COMMAND ----------

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TO DO 3: Can we further improve performance by changing order of join? 
# MAGIC
# MAGIC - Hint: Running the smaller join first means we don't have to shuffle as much data.  
# MAGIC - Can see this difference in the DAG by looking at the row count or in the stage by seeing the amount of data shuffled?

# COMMAND ----------


# Also change the join order so the smaller join runs first

joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        countries
        ON
            transactions.country_id = countries.id
    JOIN
        stores
        ON
            transactions.store_id = stores.id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Can you still see Spill in Spark UI? Note down the time taken to execute below and compare with before.  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-28-Exploded-Reduced-1-h.png)
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-28-Exploded-Reduced-2-h.png)
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-28-incorrect-estimation-h.png)
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-28-spill-removed-h.png)

# COMMAND ----------

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC Note:
# MAGIC - We put the shuffle settings back to the defaults.  Usually best to stick with the defaults which tend to improve over time.  If you hard-code configurations, you may be opting out of future performance improvements unwittingly.  Always worth checking old configurations to make sure they're still needed.  You could get big performance improvements by cleaning out old configurations.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### TO DO 4: 
# MAGIC Cost based estimator rely on statistics information to generate most efficient physical query plan with lowest cost, including choices of join strategy and order of join. 
# MAGIC
# MAGIC Run ANALYZE on the joining columns across three tables and everything will just work like a magic. Below a developer write joins without choosing the right order of join. Will Spark do the right thing? 
# MAGIC

# COMMAND ----------


# Best: Run ANALYZE on the joining columns and everything will just work

spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR COLUMNS country_id, store_id")
sql("ANALYZE TABLE stores COMPUTE STATISTICS FOR COLUMNS id")
sql("ANALYZE TABLE countries COMPUTE STATISTICS FOR COLUMNS id")

joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    JOIN
        stores
        ON
            transactions.store_id = stores.id
    JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC - What can you see in DAG of query plan? Any spill? Rows exploded? Order of Join?  
# MAGIC - Note down time taken to do the join.

# COMMAND ----------

# MAGIC %md
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.5L-CMD-34-h.png)

# COMMAND ----------

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
