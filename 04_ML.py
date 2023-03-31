# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC We previously saw how to create a DLT pipeline to ingest and prepare clickstream data to use for analytics and machine learning. We then created a feature store table containing information about our customers. This notebook will show how we can take this one step further and leverage this data to classify high risk behavior, helping us keep players safe from harm.
# MAGIC 
# MAGIC ## Step 4: Train classification model (Using XG Boost)
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-7.png" width="600"/>
# MAGIC 
# MAGIC Now that our data is prepared, we can start building a ML model to classify high risk behavior using XGBoost.
# MAGIC 
# MAGIC We'll be leveraging hyperopt to do hyperparameter tuning and find the best set of hyperparameters for our model.

# COMMAND ----------

# DBTITLE 1,Set configs
# MAGIC %run "./_resources/notebook_config"

# COMMAND ----------

from pyspark.sql.functions import col, count, countDistinct, min, mean, max, round, sum
from pyspark.sql.types import DoubleType

from databricks.feature_store import FeatureStoreClient
from databricks.feature_store import feature_table
from databricks.feature_store import FeatureLookup

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score

from hyperopt import fmin, tpe, rand, hp, Trials, STATUS_OK, SparkTrials, space_eval
from hyperopt.pyll import scope
from xgboost import XGBClassifier

import mlflow
from mlflow.models.signature import infer_signature

mlflow.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step: 4.1: Retrieve features and create training set

# COMMAND ----------

# DBTITLE 1,Create a training dataset from Feature Store
fs = FeatureStoreClient()

cust_features_df = fs.read_table(name=f"{config['database']}.customer_features").select('customer_id','is_high_risk')

feature_lookups = [
  FeatureLookup(
    table_name = f"{config['database']}.customer_features",
    feature_names = ['active_betting_days_freq','avg_daily_bets','avg_daily_wager','deposit_freq','total_deposit_amt',
                     'withdrawal_freq', 'total_withdrawal_amt', 'sports_pct_of_bets','sports_pct_of_wagers','win_rate'],
    lookup_key = ["customer_id"]
  )
]
  
training_set = fs.create_training_set(
  df=cust_features_df,
  feature_lookups = feature_lookups,
  exclude_columns=['customer_id'],
  label = "is_high_risk"
)

training_df = training_set.load_df()
display(training_df)

# COMMAND ----------

# DBTITLE 1,Create train and test data sets
features = [i for i in training_df.columns if (i != 'customer_id') & (i != 'is_high_risk')]
df = training_df.toPandas()
X_train, X_test, y_train, y_test = train_test_split(df[features], df['is_high_risk'], test_size=0.33, random_state=55)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.2: Train classification model using MLflow and Hyperopt

# COMMAND ----------

# DBTITLE 1,Define model evaluation for hyperopt
from sklearn.metrics import recall_score

def evaluate_model(params):
  #instantiate model
  model = XGBClassifier(learning_rate=params["learning_rate"],
                            gamma=int(params["gamma"]),
                            reg_alpha=int(params["reg_alpha"]),
                            reg_lambda=int(params["reg_lambda"]),
                            max_depth=int(params["max_depth"]),
                            n_estimators=int(params["n_estimators"]),
                            min_child_weight = params["min_child_weight"],
                            objective='reg:linear',
                            early_stopping_rounds=50)
  
  #train
  
  model.fit(X_train, y_train, eval_set=[(X_test, y_test)])
  
  #predict
  y_prob = model.predict_proba(X_test)
  
  #score
  precision = average_precision_score(y_test, y_prob[:,1])
  
  mlflow.log_metric('avg_precision', precision)  # record actual metric with mlflow run
  
  # return results (negative precision as we minimize the function)
  return {'loss': -precision, 'status': STATUS_OK, 'model': model}

# COMMAND ----------

# DBTITLE 1,Define search space for hyperopt
# define hyperopt search space
search_space = {'max_depth': scope.int(hp.quniform('max_depth', 2, 8, 1)),
                'learning_rate': hp.loguniform('learning_rate', -3, 0),
                'gamma': hp.uniform('gamma', 0, 5),
                'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
                'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
                'min_child_weight': scope.int(hp.loguniform('min_child_weight', -1, 3)),
                'n_estimators':  scope.int(hp.quniform('n_estimators', 50, 200, 1))}

# COMMAND ----------

# DBTITLE 1,Perform evaluation to find optimal hyperparameters
with mlflow.start_run(run_name='XGBClassifier') as run:
  trials = SparkTrials(parallelism=4)
  
  # Configure Hyperopt
  argmin = (fmin(fn=evaluate_model, 
                 space=search_space, 
                 algo=tpe.suggest, 
                 max_evals=100, 
                 trials=trials))
  
  # Identify the best trial
  model = trials.best_trial['result']['model']
  #signature = infer_signature(X_test, model.predict_proba(X_test))
  
  #Log model using the Feature Store client
  fs.log_model(
    model,
    "rmg_high_risk_classifier",
    flavor=mlflow.xgboost,#mlflow.sklearn,
    training_set=training_set,
    registered_model_name="rmg_high_risk_classifier")
  
  
  #Log hyperopt model params and our loss metric
  for p in argmin:
    mlflow.log_param(p, argmin[p])
    mlflow.log_metric("precision", trials.best_trial['result']['loss'])
  
  # Capture the run_id to use when registring our model
  run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md-sandbox 
# MAGIC 
# MAGIC **Why is it so great?**
# MAGIC 
# MAGIC <div style="float:right"><img src="https://quentin-demo-resources.s3.eu-west-3.amazonaws.com/images/tuning-2.gif" style="height: 280px; margin-left: 20px"/></div>
# MAGIC 
# MAGIC - Trials are automatically logged in MLFlow! It's then easy to compare all the runs and understand how each parameter play a role in the model
# MAGIC - Job by providing a `SparkTrial` instead of the standard `Trial`, the training and tuning is automatically paralellized in your cluster
# MAGIC - Training can easily be launched as a job and model deployment automatized based on the best model performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4.3: Save final model to registry and flag as production ready

# COMMAND ----------

# DBTITLE 1,Save our new model to registry as a new version
model_registered = mlflow.register_model("runs:/"+run_id+"/rmg_high_risk_classifier", "rmg_high_risk_classifier")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "rmg_high_risk_classifier", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Step 4.4: Use model to classify high risk behavior
# MAGIC 
# MAGIC <img style="float: right; padding-left: 10px" src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-7.png" width="600"/>
# MAGIC 
# MAGIC Now that our model is built and saved in MLFlow registry, we can load it to run our inferences at scale.
# MAGIC 
# MAGIC This can be done:
# MAGIC 
# MAGIC * In batch or streaming (ex: refresh every night)
# MAGIC   * Using a standard notebook job
# MAGIC   * Or as part of the DLT pipeline we built
# MAGIC * In real-time over a REST API, deploying Databricks serving capabilities
# MAGIC 
# MAGIC In the following cell, we'll focus on deploying the model in this notebook directly

# COMMAND ----------

# DBTITLE 1,Read data in from Feature Store
batch_df = fs.read_table(name=f"{config['database']}.customer_features").select('customer_id')

# COMMAND ----------

# DBTITLE 1,Classify behavior
predictions = fs.score_batch(
  'models:/rmg_high_risk_classifier/Production',
  batch_df)

# COMMAND ----------

# DBTITLE 1,View predictions
display(predictions.filter(col('prediction') == 1))
