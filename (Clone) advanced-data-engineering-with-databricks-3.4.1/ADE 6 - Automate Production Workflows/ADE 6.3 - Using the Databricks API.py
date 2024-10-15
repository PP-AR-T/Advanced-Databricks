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
# MAGIC
# MAGIC # Using the Databricks API
# MAGIC In this lesson, you will learn how to setup, configure, and use the Databricks API.
# MAGIC
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Programmatically run commands through the REST API using `curl` and Python

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Make API calls Using `curl`
# MAGIC
# MAGIC Use the following pattern with **`curl`** to issue API calls. Note the following:
# MAGIC * **`cat`**, in combination with a <a href="https://tldp.org/LDP/abs/html/here-docs.html" target="_blank">here document</a>, supplies the JSON payload that specifies the API parameters; in this example, we specify the **`path`** parameter.
# MAGIC * the **`-H`** option, in combination with **`DATABRICKS_TOKEN`** environment variable, specifies the authentication header
# MAGIC * the **`DATABRICKS_HOST`** environment variable supplies the base URL for the [API endpoints](https://docs.databricks.com/api).
# MAGIC * **`json_pp`** formats the JSON response into a readable form.
# MAGIC
# MAGIC This particular invocation lists the contents of the */Users* directory.

# COMMAND ----------

# MAGIC %sh cat << EOF | curl -s -X GET -H "Authorization: Bearer $DATABRICKS_TOKEN" $DATABRICKS_HOST/api/2.0/workspace/list -d @- | json_pp
# MAGIC {
# MAGIC   "path": "/Users"
# MAGIC }
# MAGIC EOF

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The commands are a bit simpler if we don't need to pass in parameters. For example, the following lists all clusters.

# COMMAND ----------

# MAGIC %sh curl -s -X GET -H "Authorization: Bearer $DATABRICKS_TOKEN" $DATABRICKS_HOST/api/2.0/clusters/list | json_pp

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Make API calls using Python
# MAGIC
# MAGIC You can issue API calls through Python. The easiest way to do this is to install and use the [Databricks SDK](https://docs.databricks.com/en/dev-tools/sdk-python.html). This is already installed for you in this setup.
# MAGIC
# MAGIC In this section, we'll see how to duplicate the two API calls from above using the Python SDK.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# Instantiate a client object. You can specify credentials in this initialization call
# but in this setup we are relying on DATABRICKS_HOST and DATABRICKS_TARGET
# having been set up.

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use the API to list contents of the */Users* folder.

# COMMAND ----------

[ i.path for i in w.workspace.list('/Users') ]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Use the API to list clusters.

# COMMAND ----------

[ (i.cluster_id,i.cluster_name) for i in w.clusters.list() ]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
