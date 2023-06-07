<img src=https://github.com/databricks-industry-solutions/.github/raw/main/profile/solacc_logo_wide.png width="600px">

[![DBR](https://img.shields.io/badge/DBR-11.3ML-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/11.3ml.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

### Responsible Gaming
The need and importance of Responsible Gaming initiatives is only going to grow as new regulation, enhanced gameplay experience, and general expansion take place in the Betting & Gaming industry. At the same time, delivering the right intervention to the right person at the right time is incredibly complex.

In this solution acelerator, we demonstrate how to identify and predict high risk behaviors to help you keep your customers safe from harm. 

To do this, we take the following steps.
1. Ingest and process synthetic gameplay data into Databricks using Delta Live Tables
2. Perform exploratory data analysis using notebook functionality and Databricks SQL
3. Create a feature store table for customer features using Databricks Feature Store
4. Train a classification model using Xgboost, Hyperopt, and MLflow
5. Perform inference to classify high risk behavior 

___
<dan.morris@databricks.com>

___


<img src="https://cme-solution-accelerators-images.s3.us-west-2.amazonaws.com/responsible-gaming/rmg-demo-flow-1.png"/>

___

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| Library Name   | Library License       | Library License URL     | Library Source URL                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Hyperopt     | BSD License (BSD) |	https://github.com/hyperopt/hyperopt/blob/master/LICENSE.txt	| https://github.com/hyperopt/hyperopt  |
| Pandas       | BSD 3-Clause License |https://github.com/pandas-dev/pandas/blob/main/LICENSE| https://github.com/pandas-dev/pandas |
| PyYAML       | MIT        | https://github.com/yaml/pyyaml/blob/master/LICENSE | https://github.com/yaml/pyyaml                      |
| Scikit-learn | BSD 3-Clause "New" or "Revised" License | https://github.com/scikit-learn/scikit-learn/blob/main/COPYING | https://github.com/scikit-learn/scikit-learn  |
|Spark         | Apache-2.0 License | https://github.com/apache/spark/blob/master/LICENSE | https://github.com/apache/spark|
| Xgboost      | Apache License 2.0 | https://github.com/dmlc/xgboost/blob/master/LICENSE | https://github.com/dmlc/xgboost  |


## Instruction

To run this accelerator, clone this repo into a Databricks workspace. Attach the `RUNME` notebook to any cluster running a DBR 11.0 or later runtime, and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. Execute the multi-step-job to see how the pipeline runs. The job configuration is written in the RUNME notebook in json format. The cost associated with running the accelerator is the user's responsibility.
