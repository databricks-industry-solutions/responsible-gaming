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
# MAGIC * To view a Data Profile report:
# MAGIC   * Run the next cell `bronze_clickstream`
# MAGIC   * Click the `+` sign that appears above the column headers
# MAGIC   * Select `Data Profile`

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
# MAGIC * See dashboard created in the last cell of the **RUNME** notebook

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC | Library Name   | Library License       | Library License URL     | Library Source URL                                              |
# MAGIC |----------------------------------------|-------------------------|------------|-----------------------------------------------------|
# MAGIC | Hyperopt     | BSD License (BSD) |	https://github.com/hyperopt/hyperopt/blob/master/LICENSE.txt	| https://github.com/hyperopt/hyperopt  |
# MAGIC | Pandas       | BSD 3-Clause License |https://github.com/pandas-dev/pandas/blob/main/LICENSE| https://github.com/pandas-dev/pandas |
# MAGIC | PyYAML       | MIT        | https://github.com/yaml/pyyaml/blob/master/LICENSE | https://github.com/yaml/pyyaml                      |
# MAGIC | Scikit-learn | BSD 3-Clause "New" or "Revised" License | https://github.com/scikit-learn/scikit-learn/blob/main/COPYING | https://github.com/scikit-learn/scikit-learn  |
# MAGIC |Spark         | Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark|
# MAGIC | Xgboost      | Apache License 2.0 | https://github.com/dmlc/xgboost/blob/master/LICENSE | https://github.com/dmlc/xgboost  |
