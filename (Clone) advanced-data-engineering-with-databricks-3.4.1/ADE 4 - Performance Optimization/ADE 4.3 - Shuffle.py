# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Shuffle
# MAGIC
# MAGIC Shuffle is a Spark mechanism that redistributes data so that it's grouped differently across partitions. This typically involves copying data across executors and machines and, while it's sometimes necessary, it can be a complex and costly operation.
# MAGIC
# MAGIC In this demo we will see shuffle in action. Run the next cell to set up the lesson.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.3

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell, which will set a Spark configuration variable that disables caching. Turning caching off makes the effect of the optimizations more apparent.

# COMMAND ----------

spark.conf.set('spark.databricks.io.cache.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Data Creation
# MAGIC
# MAGIC Let's generate the data we will use in this demo. First we'll synthesize data representing a set of sales transactions.

# COMMAND ----------

from pyspark.sql.functions import *

transactions_df = (spark
                        .range(0, 150000000, 1, 32)
                        .select(
                            'id',
                            round(rand() * 10000, 2).alias('amount'),
                            (col('id') % 10).alias('country_id'),
                            (col('id') % 100).alias('store_id')
                        )
                    )

transactions_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now we'll write the data to a table.

# COMMAND ----------

transactions_df.write.mode('overwrite').saveAsTable('transactions')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now let's synthesize data and write it to a table describing points of sale.

# COMMAND ----------

stores_df = (spark
                .range(0, 99)
                .select(
                    'id',
                    round(rand() * 100, 0).alias('employees'),
                    (col('id') % 10).alias('country_id'),
                    expr('uuid()').alias('name')
                )
            )

stores_df.display()

# COMMAND ----------

stores_df.write.saveAsTable('stores')

# COMMAND ----------

# MAGIC %md
# MAGIC Now let's create a lookup table that maps `country_id` from the data tables to an actual country name.

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

countries_df.write.saveAsTable("countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins
# MAGIC
# MAGIC Now we'll perform a query that induces shuffling by joining three tables together, writing the results to a separate table.
# MAGIC
# MAGIC **PLEASE NOTE:** in this cell, we're explicitly turning off broadcast joins in order to demonstrate shuffle.

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
    LEFT JOIN
        stores
        ON
            transactions.store_id = stores.id
    LEFT JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC Open the Spark UI to the Stages page and notice there were two 1.4GB shuffle reads: one for each join:
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.3-CMD-19-h.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcast Join
# MAGIC
# MAGIC Broadcast join avoids the shuffle. In the above cells we explicitly turned off broadcast joins, but now we'll return the configuration to the default so that broadcast join is enabled.  It only works in this case because at least one of the tables in each join are relatively small (< 100MB).

# COMMAND ----------

# Use default config & let broadcast join happen
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
spark.conf.unset("spark.databricks.adaptive.autoBroadcastJoinThreshold")

joined_df = spark.sql("""
    SELECT 
        transactions.id,
        amount,
        countries.name as country_name,
        employees,
        stores.name as store_name
    FROM
        transactions
    LEFT JOIN
        stores
        ON
            transactions.store_id = stores.id
    LEFT JOIN
        countries
        ON
            transactions.country_id = countries.id
""")

joined_df.write.mode('overwrite').saveAsTable('transact_countries')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This is an improvement. Referring back to the Spark UI **Stages** tab, note that there are no large shuffle reads anymore.  Only the small tables were shuffled, but the large table was not, so we avoided moving 1.4GB two times.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.3-CMD-20-h.png)
# MAGIC
# MAGIC Broadcast joins can be significantly faster than shuffle joins if one of the tables is very large and the other is small. Unfortunately, broadcast joins only work if at least one of the tables are less than 100MB in size. In case of joining bigger tables, if we want to avoid the shuffle we may need to reconsider our schema to avoid having to do the join in the first place.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations
# MAGIC
# MAGIC Aggregations also use a shuffle, but they're often much less expensive. The following cell executes a query that demonstrates this.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   country_id, 
# MAGIC   COUNT(*) AS count,
# MAGIC   AVG(amount) AS avg_amount
# MAGIC   FROM transactions
# MAGIC   GROUP BY country_id
# MAGIC   ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC That was fast! There are a lot of things going on here. One of the main points is that we're only shuffling the counts and sums necessary to compute the counts and averages that were requested. This only results in shuffling a few KB. Use the Spark UI once again to verify this.
# MAGIC
# MAGIC ![4](files/images/advanced-data-engineering-with-databricks-3.4.1/4.3-CMD-21-h.png)
# MAGIC
# MAGIC So the shuffle is cheap compared to the shuffle joins, which need to shuffle all of the data. It also helps that our output is basically 0 in this case.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
