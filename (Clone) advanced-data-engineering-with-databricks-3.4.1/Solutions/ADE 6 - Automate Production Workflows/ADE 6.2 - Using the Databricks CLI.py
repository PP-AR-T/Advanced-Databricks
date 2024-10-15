# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Databricks CLI
# MAGIC In this lesson, you will execute commands using the Databricks CLI. We will use the cluster web terminal for this demo.
# MAGIC
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Launch the driver's web terminal to run shell commands on the driver node
# MAGIC * Install the Databricks CLI and configure authentication to a Databricks workspace
# MAGIC * List files or clusters in your workspace to verify successful authentication
# MAGIC * Configure Databricks Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC Run the classroom setup script in the next cell to configure the classroom.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-06.2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Install CLI
# MAGIC
# MAGIC Install the Databricks CLI using the following cell. Note that this procedure removes any existing version that may already be installed, and installs the newest version of the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html). A legacy version exists that is distributed through **`pip`**, however we recommend following the procedure here to install the newer one.

# COMMAND ----------

# MAGIC %sh rm -f $(which databricks); curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.211.0/install.sh | sh

# COMMAND ----------

# MAGIC %md
# MAGIC ## Authentication
# MAGIC
# MAGIC Usually, you would have to set up authentication for the CLI. But in this training environment, that's already taken care of if you ran through the accompanying *Generate Tokens* notebook. If you did, credentials will already be loaded into the **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** environment variables. If you did not, run through it now then restart this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using the CLI
# MAGIC
# MAGIC The cell below illustrates simple CLI usage. This displays a lists the contents of in */Users* workspace directory.

# COMMAND ----------

# MAGIC %sh databricks workspace list /Users

# COMMAND ----------

# MAGIC %md
# MAGIC The CLI provides access to a broad swath of Databricks functionality. Use the **`help`** command to access the online help.

# COMMAND ----------

# MAGIC %sh databricks help

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
