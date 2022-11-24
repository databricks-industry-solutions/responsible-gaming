# Databricks notebook source
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username_sql_compatible = useremail.split('@')[0].replace(".", "_")

# COMMAND ----------

config = {
  "database": "SOLACC_real_money_gaming",
  "experiment_path": f"/Users/{useremail}/real_money_gaming",
  "data_path": f"/databricks_solacc/real_money_gaming/data", 
  "pipeline_path": f"/databricks_solacc/real_money_gaming/dlt",
  "pipeline_name": "SOLACC_real_money_gaming"
}

# COMMAND ----------

# DBTITLE 1,Create source data path
dbutils.fs.mkdirs(f"{config['data_path']}/raw")

# COMMAND ----------

# DBTITLE 1,Set current database
_ = spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['database']}")
_ = spark.sql(f"USE {config['database']}")

# COMMAND ----------

import mlflow

mlflow.set_experiment(config['experiment_path']) 
