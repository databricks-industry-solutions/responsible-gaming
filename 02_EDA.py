# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Step 2: Exploratory Data Analysis
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-4.png" width="700"/>
# MAGIC 
# MAGIC Now that we've streamed in and parsed our data, it's time to conduct exploratory data analysis (EDA).
# MAGIC 
# MAGIC In practice, EDA can be done in a number of different ways.
# MAGIC - Leverage data profiling capabilities within notebooks
# MAGIC - Build a dashboard using Databricks SQL or other BI tool
# MAGIC - Ad hoc queries and visualizations
# MAGIC 
# MAGIC In this notebook, we'll explore the first two: data profiling within a notebook and Databricks SQL

# COMMAND ----------

# MAGIC %run "./_resources/notebook_config"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.1: Data profiling within a notebook

# COMMAND ----------

# DBTITLE 1,bronze_clickstream
# MAGIC %sql
# MAGIC select * from bronze_clickstream

# COMMAND ----------

# DBTITLE 1,silver_bets
# MAGIC %sql
# MAGIC select * from silver_bets

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_withdrawals

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2.2: Databricks SQL
# MAGIC * See dashboard created in the **RUNME** notebook
