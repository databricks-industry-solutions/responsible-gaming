# Databricks notebook source
# MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/real-money-gaming. For more information about this solution accelerator, visit https://www.databricks.com/solutions/accelerators/responsible-gaming.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC The need and importance of Responsible Gaming initiatives is only going to grow as new regulation, enhanced gameplay experience, and general expansion take place in the Betting & Gaming industry. At the same time, delivering the right intervention to the right person at the right time is incredibly complex.
# MAGIC
# MAGIC In this solution acelerator, we demonstrate how to identify and predict high risk behaviors to help you keep your customers safe from harm. 
# MAGIC
# MAGIC To do this, we take the following steps.
# MAGIC 1. Ingest and process synthetic gameplay data into Databricks using Delta Live Tables
# MAGIC 2. Perform exploratory data analysis using notebook functionality and Databricks SQL
# MAGIC 3. Create a feature store table for customer features using Databricks Feature Store
# MAGIC 4. Train a classification model using Xgboost, Hyperopt, and MLflow
# MAGIC 5. Perform inference to classify high risk behavior 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Step 1: Data Ingestion with Delta Live Tables
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-1.png" width="700"/>
# MAGIC
# MAGIC To simplify the ingestion process and accelerate our developments, we'll leverage [Delta Live Tables (DLT)](https://www.databricks.com/product/delta-live-tables).
# MAGIC
# MAGIC DLT lets you declare your transformations and will handle the Data Engineering complexity for you:
# MAGIC
# MAGIC - Data quality tracking with expectations
# MAGIC - Continuous or scheduled ingestion, orchestrated as pipeline
# MAGIC - Build lineage and manage data dependencies
# MAGIC - Automating scaling and fault tolerance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1: Create and run workflow using RUNME
# MAGIC Please open the RUNME file in this repo and follow the steps listed for running the notebooks in this accelerator.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2: Create and run Delta Live Tables pipeline
# MAGIC In step 2 of the RUNME file, you were given the option to run the workflow as a multi-step job (2a) or to run the notebooks interactively (2b). 
# MAGIC
# MAGIC * If you opted for running the workflow as a multi-step job (2a), then the code in this notebook has already been run and you can move on to the next step. 
# MAGIC
# MAGIC * If you opted for running the notebooks interactively (2b), you will now need to create a Delta Live Tables pipeline.
# MAGIC    * Click on `Workflows` in the left panel
# MAGIC    * Click on `Delta Live Tables` in the top nav
# MAGIC    * Click `Create Pipeline`
# MAGIC      * `Pipeline Name` - this can be anything you choose. We suggest `SOLACC_real_money_gaming`
# MAGIC      * `Pipeline Mode` - select 'Triggered' as this only needs to be run one time.
# MAGIC      * `Source Code` - click on the folder icon and navigate to the name of this notebook. The path will be similar to `/Repos/UserName/real-money-gaming/01_DLT`
# MAGIC      * `Storage Location` - enter `dbfs:/databricks_solacc/real_money_gaming/dlt`. This is the location that data will be written out to when creating silver/gold tables.
# MAGIC      * `Target Schema` - enter `SOLACC_real_money_gaming`. This is the name of the database that we will store our tables in.
# MAGIC  
# MAGIC * Please note that the code below must be run using a Delta Live Tables pipeline and will fail if you try to run it interactively.
# MAGIC      
# MAGIC Click [here](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-ui.html) for more information on creating, running, and managing Delta Live Tables pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3: Stream in-game clickstream data into Delta Lake
# MAGIC
# MAGIC In this step, we use Delta Live Tables to load our raw clickstream data into the table `bronze_clickstream.` As you'll notice, we've applied the `@dlt.table decorator` to our function `bronze_clickstream`, which instructs DLT to create a table using the data returned by the function. 
# MAGIC
# MAGIC Visit our [Delta Live Tables Python language reference](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html) for more details on DLT syntax 

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, min, mean, max, round, sum, lit
import dlt

# COMMAND ----------

schema = 'customer_id STRING, age_band STRING, gender STRING, date STRING, date_transaction_id INT, event_type STRING, game_type STRING, wager_amount FLOAT, win_loss STRING, win_loss_amount FLOAT, initial_balance FLOAT, ending_balance FLOAT, withdrawal_amount FLOAT, deposit_amount FLOAT'

# COMMAND ----------

# DBTITLE 1,bronze_clickstream
@dlt.table
def bronze_clickstream():
  raw_data_path = f's3a://db-gtm-industry-solutions/data/CME/real_money_gaming/data/raw/*'
  return spark.read.csv(raw_data_path,schema=schema)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1.4: Create a silver table for each beacon
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
  return (dlt.read("bronze_clickstream").select('customer_id', 'date', 'date_transaction_id',
          'event_type','game_type','wager_amount','win_loss','win_loss_amount','initial_balance','ending_balance')
          .filter(col('event_type') == 'bet'))

# COMMAND ----------

# DBTITLE 1,silver_deposits
@dlt.table
def silver_deposits():
  return (dlt.read("bronze_clickstream").select('customer_id', 'date', 'date_transaction_id','event_type','initial_balance','ending_balance','deposit_amount')
         .filter(col('event_type') == 'deposit'))

# COMMAND ----------

# DBTITLE 1,silver_flagged_high_risk
@dlt.table
def silver_flagged_high_risk():
  return (dlt.read("bronze_clickstream").select('customer_id', 'date','event_type')
         .filter(col('event_type') == 'flagged_high_risk'))

# COMMAND ----------

# DBTITLE 1,silver_registrations
@dlt.table
def silver_registrations():
  return (dlt.read("bronze_clickstream").select('customer_id', 'date','event_type','gender','age_band')
         .filter(col('event_type') == 'register'))

# COMMAND ----------

# DBTITLE 1,silver_withdrawals
@dlt.table
def silver_withdrawals():
  return (dlt.read("bronze_clickstream").select('customer_id', 'date','date_transaction_id', 'event_type','initial_balance','ending_balance','withdrawal_amount')
         .filter(col('event_type') == 'withdrawal'))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 1.5: Create Gold Table
# MAGIC
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-3.png" width="700"/>
# MAGIC
# MAGIC Once our Silver tables are ready, we'll merge the information they contain into a final daily activity Gold table, ready for data analysis and data science.

# COMMAND ----------

# DBTITLE 1,gold_daily_activity
@dlt.table
def gold_daily_activity():
  daily_betting_activity = (dlt.read('silver_bets').groupBy('customer_id','date')
                            .agg(count('date_transaction_id').alias('num_bets'),
                                sum('wager_amount').alias('total_wagered'),
                                min('wager_amount').alias('min_wager'),
                                max('wager_amount').alias('max_wager'),
                                round(mean('wager_amount'),2).alias('mean_wager'),
                                round(sum('win_loss_amount'),2).alias('winnings_losses')))

  daily_deposits = (dlt.read('silver_deposits').groupBy('customer_id','date')
                    .agg(count('event_type').alias('num_deposits'), sum('deposit_amount').alias('total_deposit_amt')))

  daily_withdrawals = (dlt.read('silver_withdrawals').groupBy('customer_id','date')
                       .agg(count('event_type').alias('num_withdrawals'), sum('withdrawal_amount').alias('total_withdrawal_amt')))
  
  
  daily_high_risk_flags = (dlt.read('silver_flagged_high_risk').withColumn('is_high_risk',lit(1)).drop('event_type'))

  return (daily_betting_activity.join(daily_deposits,on=['customer_id','date'],how='outer')
          .join(daily_withdrawals,on=['customer_id','date'],how='outer').join(daily_high_risk_flags,on=['customer_id', 'date'],how='outer').na.fill(0))

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
