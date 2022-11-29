# Databricks notebook source
displayHTML(f'''<div style="width:1150px; margin:auto"><iframe src="https://docs.google.com/presentation/d/1EK-VchMyyiyJ3jS7uVnxgQlj7lYQ-ozBlUqvzuE8GCo/embed?slide=1
" frameborder="0" width="800" height="500"></iframe></div>''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### About the Data
# MAGIC This solution accelerator uses a synthetic data set that was designed to represent clickstream data collected from a RMG website/app. This data is sent through in the form of five beacon types:
# MAGIC * **Registrations:** customer creates new account with service.
# MAGIC * **Bets:** customer places wager on their game of choice.
# MAGIC * **Deposits:** customer deposits money into their account for betting.
# MAGIC * **Withdrawals:** customer withdraws money from their account.
# MAGIC * **Flagged High Risk:** customer is flagged as high risk through standard operating procedures.

# COMMAND ----------

# MAGIC %md But first, let's set up the configs and reinitialize the source data.

# COMMAND ----------

# MAGIC %run "./_resources/notebook_config"

# COMMAND ----------

config

# COMMAND ----------

# DBTITLE 1,Initialize source data if it does not exist
if len(dbutils.fs.ls(f"{config['data_path']}/raw")) == 0:
  dbutils.fs.cp("s3a://db-gtm-industry-solutions/data/CME/real_money_gaming/data/raw", f"{config['data_path']}/raw", True)
