# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC ###Step 3: Create table in Feature Store for customer features
# MAGIC
# MAGIC #### Feature Store
# MAGIC
# MAGIC The [Databricks Feature Store](https://docs.databricks.com/applications/machine-learning/feature-store/index.html) provides data teams with the ability to create new features, explore and reuse existing ones, publish features to low-latency online stores, build training sets and retrieve feature values for batch inference.
# MAGIC
# MAGIC Key benefits of the Databricks Feature Store:
# MAGIC * **Discoverabilty:** teams can't reuse what they can't find, so one purpose of feature stores is discovery, or surfacing features that have already been usefully refined from raw data. With the Databricks Feature Store UI, features can be easily browsed and searched for within the Databricks workspace.
# MAGIC * **Lineage:** reusing a feature computed for one purpose means that changes to its computation now affect many consumers. Detailed visibility into upstream and downstream lineage means that feature producers and consumers can reliably share and reuse features within an organization.
# MAGIC * **Integration with model scoring and serving:** when you use features from Databricks Feature Store to train a model, the model is packaged with feature metadata. When you use the model for batch scoring or online inference, it automatically retrieves features from Feature Store. The caller does not need to know about them or include logic to look up or join features to score new data. This makes model deployment and updates much easier.

# COMMAND ----------

# DBTITLE 1,Set configs
# MAGIC %run "./_resources/notebook_config"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ###Step 3.1: Load tables to compute features
# MAGIC
# MAGIC #### Crafting our features
# MAGIC
# MAGIC <img src='https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-5.png' width='700' style="float: right">
# MAGIC
# MAGIC Many of the customer features that are useful for managing responsible gaming are useful for other use cases as well, such as predicting propensity to churn and personalizating marketing communications.
# MAGIC
# MAGIC This reusability makes customer features a prime candidate for a feature store table. 
# MAGIC
# MAGIC In this step, we will create a customer_features table containing the following:
# MAGIC * **Demographic information** such as age band and gender.
# MAGIC * **Account activity** such as registration, deposits, and withdrawals.
# MAGIC * **Betting activity** such as game type, wager, wins and losses, and so on.

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, min, mean, max, round, sum, datediff
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
from databricks.feature_store import FeatureLookup

# COMMAND ----------

# DBTITLE 1,Load tables to compute customer features
registrations_df = spark.table('silver_registrations').select('customer_id','gender','age_band')
daily_activity_df = spark.table('gold_daily_activity')
bets_df = spark.table('silver_bets')

# COMMAND ----------

# DBTITLE 1,View gold_daily_activity table
display(spark.table('gold_daily_activity'))

# COMMAND ----------

# DBTITLE 1,View silver_registrations table
display(spark.table('silver_registrations'))

# COMMAND ----------

# DBTITLE 1,View silver_bets
display(spark.table('silver_bets'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.2: Define and compute customer features

# COMMAND ----------

# DBTITLE 1,Define customer features
from pyspark.sql.functions import when

def compute_features(registrations_df,bets_df,daily_activity_df):
  # Compute aggregate metrics for each customer
  activity_agg_df = (daily_activity_df.groupBy('customer_id').agg(count('date').alias('active_betting_days'),
       sum('num_bets').alias('total_num_of_bets'),
       round(mean('num_bets'),2).alias('avg_daily_bets'),
       sum('total_wagered').alias('total_wagered_amt'),
       round(mean('total_wagered'),2).alias('avg_daily_wager'),
       sum('winnings_losses').alias('total_win_loss_amount'),
       sum(when(col('winnings_losses') > 0,1).otherwise(0)).alias('total_num_of_wins'),
       sum('num_deposits').alias('total_num_of_deposits'),
       sum(when(col('num_deposits') > 0,1).otherwise(0)).alias('active_deposit_days'),                                                        
       round(sum('total_deposit_amt'),2).alias('total_deposit_amt'),
       sum('num_withdrawals').alias('total_num_of_withdrawals'),
       sum(when(col('num_withdrawals') > 0,1).otherwise(0)).alias('active_withdrawal_days'), 
       round(sum('total_withdrawal_amt'),2).alias('total_withdrawal_amt'),
       min('date').alias('registration_date'),
       max('date').alias('last_active_date'),                                                         
       sum('is_high_risk').alias('is_high_risk'))
       .withColumn('win_rate',round(col('total_num_of_wins')/col('total_num_of_bets'),2))
       .withColumn('deposit_freq',round(col('active_deposit_days')/col('active_betting_days'),2))
       .withColumn('withdrawal_freq',round(col('active_withdrawal_days')/col('active_betting_days'),2))
       .withColumn('lifetime_days',datediff(col('last_active_date'),col('registration_date')))
       .withColumn('active_betting_days_freq',round(col('active_betting_days')/col('lifetime_days'),2)))
  
  # Compute proportion of bets and wagers for Sports Betting
  sports_agg_df = (bets_df.groupBy('customer_id','game_type')
       .agg(count('wager_amount').alias('num_bets'), sum('wager_amount').alias('total_wager'), 
          sum('win_loss_amount').alias('sports_total_win_loss_amt'),
          sum(when(col('win_loss') == 'win',1).otherwise(0)).alias('sports_wins')                                         )
       .filter(col('game_type') == 'sports betting').drop('game_type')
       .withColumnRenamed('num_bets','sports_num_bets').withColumnRenamed('total_wager','sports_total_wagered')
       .withColumn('sports_win_rate',round(col('sports_wins')/col('sports_num_bets'),2)))
  
  # Join the three tables and add additional proportion of bets/wagers columns for sports and casino
  agg_df = (registrations_df.join(activity_agg_df,on='customer_id',how='leftouter').join(sports_agg_df,on='customer_id',how='leftouter')
       .withColumn('sports_pct_of_bets',round(col('sports_num_bets')/col('total_num_of_bets'),2))
       .withColumn('sports_pct_of_wagers',round(col('sports_total_wagered')/col('total_wagered_amt'),2))
       .withColumn('casino_num_of_bets',(col('total_num_of_bets')-col('sports_num_bets')))
       .withColumn('casino_total_wagered',(col('total_wagered_amt')-col('sports_total_wagered')))
       .withColumn('casino_pct_of_bets',round(col('casino_num_of_bets')/col('total_num_of_bets'),2))
       .withColumn('casino_pct_of_wagers',round(col('casino_total_wagered')/col('total_wagered_amt'),2)).na.fill(0))
  
  return agg_df

# COMMAND ----------

# DBTITLE 1,Compute customer features
customer_features = compute_features(registrations_df,bets_df,daily_activity_df)
display(customer_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3.3: Create feature store table

# COMMAND ----------

# DBTITLE 1,Create customer_features feature table
fs = FeatureStoreClient()

# COMMAND ----------

try:
  fs.drop_table(
    name=f"{config['database']}.customer_features" # throws value error if Feature Store table does not exist
  )
except ValueError: 
  pass

fs.create_table(
  name=f"{config['database']}.customer_features",
  description="Customer demographics and activity features",
  tags={"hasPII":"False"},
  primary_keys=["customer_id"],
  df=customer_features)

# COMMAND ----------

# DBTITLE 1,Read customer_features table from feature store
display(fs.read_table(name=f"{config['database']}.customer_features"))

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
