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
# MAGIC ### Unit Test Workouts

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test silver table transformations

# COMMAND ----------

@dlt.table(
    # temporary=True,
    comment="Test: check silver table removes null workout ids and has correct count"
)
@dlt.expect_all({
    "keep_all_rows": "num_rows == 5",
    "null_ids_removed": "null_ids == 0"
})
def test_workouts_clean():
    return (
        dlt.read("workouts_silver")
            .select("*", F.col("workout_id").isNull().alias("workout_id_null"))
            .select(
                F.count("*").alias("num_rows"), 
                F.sum(F.col("workout_id_null").cast("int")).alias("null_ids"))
        )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test primary key uniqueness

# COMMAND ----------

@dlt.table(
    # temporary=True,
    comment="Test: check that gold table only contains unique workout id"
)
@dlt.expect_all({
    "pk_must_be_unique": "duplicate == 1"
})
def test_workouts_completed():
    return ( 
        dlt.read("workouts_completed")
            .groupby("workout_id")
            .agg(F.count("workout_id").alias("duplicate"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
