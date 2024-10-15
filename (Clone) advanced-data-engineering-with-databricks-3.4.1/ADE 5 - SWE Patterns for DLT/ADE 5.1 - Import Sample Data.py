# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Import Sample Data
# MAGIC This notebook imports a Python module used to share sample datasets.
# MAGIC
# MAGIC ##### Objectives
# MAGIC - Install Python library using %pip
# MAGIC - Import a module from workspace files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Sample Datasets
# MAGIC
# MAGIC The `test_data_setup` module was created to modularize the code that prepared sample records for our datasets. We will import this module to get the sample data and description using this `test_data` class defined [here](https://github.com/databricks-academy/advanced-data-engineering-test-data-example/blob/published/src/data_setup/test_data_setup.py). This module was packaged in a library and released as a Python wheel file [here](https://github.com/databricks-academy/advanced-data-engineering-test-data-example/releases), which can be installed using the `%pip` command. Alternatively, this module can be stored as relative library using Files in Repos and imported directly using relative paths.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Python Library with %pip
# MAGIC In the next two cells, we generate the install command and run **`%pip`** to install the package.
# MAGIC
# MAGIC **NOTE:** You should always place all **`%pip`** commands at the beginning of the notebook, since they modify the execution environment and reset the notebook state. Any Python methods or variables created in a notebook are lost when you run **`%pip`**.

# COMMAND ----------

version = "0.0.5"
data_library_url = f"https://github.com/databricks-academy/advanced-data-engineering-test-data-example/releases/download/v{version}/data_setup-{version}-py3-none-any.whl"
pip_command = f"install --quiet --disable-pip-version-check {data_library_url}"

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %md
# MAGIC We can now import the module for our test data from the package installed above.

# COMMAND ----------

from data_setup import test_data_setup

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Import Relative Libraries
# MAGIC
# MAGIC Alternatively, we can store this module in a relative library using Files in Repos, and import the module directly using relative paths. This can be helpful for ensuring that our notebook always uses the correct version.
# MAGIC
# MAGIC For example, say the module code from the wheel package installed above was refactored into a Python file stored in the same repo and directory as this notebook. If this file was called `test_data_setup.py`, we can now import this without installing any packages, with the following import statement.
# MAGIC
# MAGIC ```
# MAGIC import test_data_setup
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Sample Data
# MAGIC
# MAGIC Let's use our imported module to access our sample datasets.
# MAGIC
# MAGIC The code below uses the `test_data` class to get sample data, print one of the datasets (workouts), and calls a method to view a description of all sample datasets provided by this module.

# COMMAND ----------

data = test_data_setup.test_data()

print(data.workouts_json)
print(data.get_data_description())

# COMMAND ----------

import pandas as pd

pdf = pd.DataFrame(data.workouts_json.split("\n"), columns=["data"])
df = spark.createDataFrame(pdf)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use this to create our sample dataset.

# COMMAND ----------

from pyspark.sql.functions import col, from_json

schema = "user_id INT, workout_id INT, timestamp FLOAT, action STRING, session_id INT"
df = df.select(from_json(col("data"), schema).alias("json")).select("json.*")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can also import Python libraries in DLT pipelines, which we'll demonstrate in a later lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
