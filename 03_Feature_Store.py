# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC ###Step 3: Create table in Feature Store for customer features
# MAGIC 
# MAGIC #### Feature Store
# MAGIC 
# MAGIC The [Databricks Feature Store](https://docs.databricks.com/applications/machine-learning/feature-store/index.html) provides data teams with the ability to create new features, explore and reuse exist ones, publish features to low-latency online stores, build training sets and retrieve feature values for batch inference.
# MAGIC 
# MAGIC Key benefits of the Databricks Feature Store:
# MAGIC * **Discoverabilty:** teams can't reuse what they can't find, so one purpose of feature stores is discovery, or surfacing features that have already been usefully refined from raw data. With the Databricks Feature Store UI, features can be easily browsed and search for within the Databricks workspace.
# MAGIC * **Lineage:** reusing a feature computered for one purpose means that changes to its computation now affect many consumers. Detailed visibility into upstream and downstream lineage means that feature producers and consumers can reliably share and reuse features within an organization.
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
# MAGIC Many of the customer features that are useful for managing responisble gaming are useful for other use cases as well, such as predicting propensity to churn and personalizating marketing communications.
# MAGIC 
# MAGIC This reusability makes customer features a prime candidate for a feature store table. 
# MAGIC 
# MAGIC In this step, we will create a customer_features table containing the following:
# MAGIC * **Demographic information** such as age band and gender.
# MAGIC * **Account activity** such as registration, deposits, and withdrawals.
# MAGIC * **Betting activity** such as game type, wager, theoretical loss, and so on.

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, min, mean, max, round, sum, datediff
from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
from databricks.feature_store import FeatureLookup

# COMMAND ----------

# DBTITLE 1,Load tables to compute customer features
registrations_df = spark.table('silver_registrations').select('customerId','gender','ageBand')
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
def compute_features(registrations_df,bets_df,daily_activity_df):
  # Compute aggregate metrics for each customer
  activity_agg_df = (daily_activity_df.groupBy('customerId').agg(count('date').alias('activeBettingDays'),
        sum('numBets').alias('totalNumOfBets'),
        round(mean('numBets'),2).alias('avgDailyBets'),
        sum('totalWagered').alias('totalWageredAmt'),
        round(mean('totalWagered'),2).alias('avgDailyWager'),
        sum('numDeposits').alias('activeDepositDays'),
        round(sum('totalDepositAmt'),2).alias('totalDepositAmt'),
        sum('numWithdrawals').alias('activeWithdrawalDays'),
        round(sum('totalWithdrawalAmt'),2).alias('totalWithdrawalAmt'),
        min('date').alias('registrationDate'),
        max('date').alias('lastActiveDate'),                                                         
        sum('isHighRisk').alias('isHighRisk'))
  .withColumn('depositFreq',round(col('activeDepositDays')/col('activeBettingDays'),2))
  .withColumn('withdrawalFreq',round(col('activeWithdrawalDays')/col('activeBettingDays'),2))
  .withColumn('lifetimeDays',datediff(col('lastActiveDate'),col('registrationDate')))
  .withColumn('activeBettingDaysFreq',round(col('activeBettingDays')/col('lifetimeDays'),2)))
  
  # Compute proportion of bets and wagers for Sports Betting
  sports_agg_df = (bets_df.groupBy('customerId','gameType').agg(count('wager').alias('numBets'),
        sum('wager').alias('totalWager')).filter(col('gameType') == 'Sports Betting').drop('gameType')
       .withColumnRenamed('numBets','sportsNumBets').withColumnRenamed('totalWager','sportsTotalWager'))
  
  # Join the three tables and add additional proportion of bets/wagers columns for sports and casino
  agg_df = (registrations_df.join(activity_agg_df,on='customerId',how='leftouter').join(sports_agg_df,on='customerId',how='leftouter')
       .withColumn('sportsPctOfBets',round(col('sportsNumBets')/col('totalNumOfBets'),2))
       .withColumn('sportsPctOfWagers',round(col('sportsTotalWager')/col('totalWageredAmt'),2))
       .withColumn('casinoNumOfBets',(col('totalNumOfBets')-col('sportsNumBets')))
       .withColumn('casinoTotalWager',(col('totalWageredAmt')-col('sportsTotalWager')))
       .withColumn('casinoPctOfBets',round(col('casinoNumOfBets')/col('totalNumOfBets'),2))
       .withColumn('casinoPctOfWagers',round(col('casinoTotalWager')/col('totalWageredAmt'),2)).na.fill(0))
  
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
  primary_keys=["customerId"],
  df=customer_features)

# COMMAND ----------

# DBTITLE 1,Read customer_features table from feature store
display(fs.read_table(name=f"{config['database']}.customer_features"))
