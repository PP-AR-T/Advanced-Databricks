# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy a Pipeline with the Databricks CLI
# MAGIC
# MAGIC In this lesson, you will learn how to manage pipelines using the Databricks CLI.
# MAGIC
# MAGIC By the end of this lesson, you will be able to:
# MAGIC * Trigger a pipeline update
# MAGIC * Query a pipeline
# MAGIC * Clone a pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run the setup
# MAGIC Run the setup script for this lesson by running the cell below. This will ensure that:
# MAGIC * The Databricks CLI is installed
# MAGIC * Authentication is configured
# MAGIC * A pipeline is created

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger a pipeline update
# MAGIC
# MAGIC Use the following command to start the pipeline. Note that it uses an environment variable named **`DATABRICKS_PIPELINE_ID`** that was populated as part of the setup.

# COMMAND ----------

# MAGIC %sh databricks pipelines start-update $DATABRICKS_PIPELINE_ID --full-refresh

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use the following command to view the current status of the pipeline run. Repeat as needed.

# COMMAND ----------

# MAGIC %sh databricks pipelines get $DATABRICKS_PIPELINE_ID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone a pipeline
# MAGIC Cloning a pipeline using the CLI involves getting the settings for a pipeline, removing elements of the settings that are not needed, changing the name of the pipeline, and creating a new pipeline with the changed settings. The commands below perform all of these actions.
# MAGIC
# MAGIC Note the following:
# MAGIC * We use the **`get`** command to get the JSON output of the existing pipeline into a file named *settings.json*
# MAGIC * We process this output in a Python script that performs the following transformations:
# MAGIC     * Keeps only the **`spec`** portion of the configuration
# MAGIC     * Deletes the existing **`id`** (a new one will be created when the new pipeline is created)
# MAGIC     * Adjusts names for the pipeline itself and the target by appending with **`_copy`**

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p var && databricks pipelines get $DATABRICKS_PIPELINE_ID > var/settings.json
# MAGIC python << EOF 
# MAGIC import json
# MAGIC
# MAGIC with open("var/settings.json", "r") as f:
# MAGIC     settings = json.load(f)['spec']
# MAGIC
# MAGIC del settings['id']
# MAGIC settings['name'] = settings['name'] + '_copy'
# MAGIC settings['target'] = settings['target'] + '_copy'
# MAGIC
# MAGIC with open("var/settings.json", "w") as f:
# MAGIC     json.dump(settings, f, indent=2)
# MAGIC EOF

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### View settings
# MAGIC
# MAGIC Use the following command to display the *settings.json* file. Since the file is created in your workspace, you could also display it through the workspace user interface, if desired.

# COMMAND ----------

# MAGIC %sh cat var/settings.json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create a new pipeline
# MAGIC
# MAGIC Use the following command to create a new pipeline based on the *settings.json* file.

# COMMAND ----------

# MAGIC %sh databricks pipelines create --json @var/settings.json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Run the new pipeline
# MAGIC
# MAGIC Run the new pipeline, ensuring first that you copy the value for **`pipeline_id`** from the cell above into the cell below.

# COMMAND ----------

# MAGIC %sh databricks pipelines start-update "pipeline_id" --full-refresh

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query status
# MAGIC
# MAGIC Use the following command to view the current status of the cloned pipeline, again substituting **`pipeline_id`**. Repeat as needed.

# COMMAND ----------

# MAGIC %sh databricks pipelines get "pipeline_id"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delete the pipeline
# MAGIC
# MAGIC Delete the cloned pipeline with the following command, again substituting **`pipeline_id`**.

# COMMAND ----------

# MAGIC %sh databricks pipelines delete "pipeline_id"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
