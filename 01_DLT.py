# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ## Step 1: Data Ingestion with Delta Live Tables
# MAGIC 
# MAGIC To simplify the ingestion process and accelerate our developments, we'll leverage Delta Live Table (DLT).
# MAGIC 
# MAGIC DLT lets you declare your transformations and will handle the Data Engineering complexity for you:
# MAGIC 
# MAGIC - Data quality tracking with expectations
# MAGIC - Continuous or scheduled ingestion, orchestrated as pipeline
# MAGIC - Build lineage and manage data dependencies
# MAGIC - Automating scaling and fault tolerance
# MAGIC 
# MAGIC ### Step 1.1: Stream in-game clickstream data into Delta Lake
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-1.png" width="700"/>
# MAGIC 
# MAGIC Our raw data is being sent to a blob storage. We'll use Databricks autoloader to ingest this information.
# MAGIC 
# MAGIC Autoloader simplify ingestion, including schema inference, schema evolution while being able to scale to millions of incoming files.

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, min, mean, max, round, sum, lit
import dlt

# COMMAND ----------

data_path = spark.conf.get("data.path")

# COMMAND ----------

# DBTITLE 1,bronze_clickstream
@dlt.table
def bronze_clickstream():
  raw_data_path = f'{data_path}raw/'
  return spark.read.json(raw_data_path)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1.2: Create a silver table for each beacon
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-2.png" width="700"/>
# MAGIC 
# MAGIC The next step is to parse the incoming bronze table and build one silver table for each beacon type
# MAGIC * **Bets:** customer places wager on their game of choice.
# MAGIC * **Deposits:** customer deposits money into their account for betting.
# MAGIC * **Flagged High Risk:** customer is flagged as high risk through standard operating procedures.
# MAGIC * **Registrations:** customer creates new account with service.
# MAGIC * **Withdrawals:** customer withdraws money from their account.

# COMMAND ----------

# DBTITLE 1,silver_bets
@dlt.table
def silver_bets():
  return (dlt.read("bronze_clickstream").select('customerId', 'date', 'eventType', 'dateBetNumber', 'gameType', 'theoreticalLoss', 'wager')
          .filter(col('eventType') == 'bet'))

# COMMAND ----------

# DBTITLE 1,silver_deposits
@dlt.table
def silver_deposits():
  return (dlt.read("bronze_clickstream").select('customerId', 'date','eventType','depositAmount')
         .filter(col('eventType') == 'deposit'))

# COMMAND ----------

# DBTITLE 1,silver_flaggedHighRisk
@dlt.table
def silver_flaggedHighRisk():
  return (dlt.read("bronze_clickstream").select('customerId', 'date','eventType')
         .filter(col('eventType') == 'flaggedHighRisk'))

# COMMAND ----------

# DBTITLE 1,silver_registrations
@dlt.table
def silver_registrations():
  return (dlt.read("bronze_clickstream").select('customerId', 'date','eventType','gender','ageBand')
         .filter(col('eventType') == 'register'))

# COMMAND ----------

# DBTITLE 1,silver_withdrawals
@dlt.table
def silver_withdrawals():
  return (dlt.read("bronze_clickstream").select('customerId', 'date','eventType','withdrawalAmount')
         .filter(col('eventType') == 'withdrawal'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1.3: Create Gold Table
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-3.png" width="700"/>
# MAGIC 
# MAGIC Once our Silver tables are ready, we'll merge the information they contain into a final daily activity Gold table, ready for data analysis and data science.

# COMMAND ----------

# DBTITLE 1,gold_daily_activity
@dlt.table
def gold_daily_activity():
  daily_betting_activity = (dlt.read('silver_bets').groupBy('customerId','date')
                            .agg(count('dateBetNumber').alias('numBets'),
                                sum('wager').alias('totalWagered'),
                                min('wager').alias('minWager'),
                                max('wager').alias('maxWager'),
                                round(mean('wager'),2).alias('meanWager'),
                                round(sum('theoreticalLoss'),2).alias('theoreticalLoss')))

  daily_deposits = (dlt.read('silver_deposits').groupBy('customerId','date')
                    .agg(count('eventType').alias('numDeposits'), sum('depositAmount').alias('totalDepositAmt')))

  daily_withdrawals = (dlt.read('silver_withdrawals').groupBy('customerId','date')
                       .agg(count('eventType').alias('numWithdrawals'), sum('withdrawalAmount').alias('totalWithdrawalAmt')))
  
  
  daily_high_risk_flags = (dlt.read('silver_flaggedHighRisk').withColumn('isHighRisk',lit(1)).drop('eventType'))

  return (daily_betting_activity.join(daily_deposits,on=['customerId','date'],how='outer')
          .join(daily_withdrawals,on=['customerId','date'],how='outer').join(daily_high_risk_flags,on=['customerId', 'date'],how='outer').na.fill(0))
