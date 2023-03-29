# Databricks notebook source
from multiprocessing.pool import ThreadPool

# Input parameters
num_threads = 40
notebook_name = './00_Generator'
timeout_seconds = 60000

# Set Thread Pool
pool = ThreadPool(num_threads)

# Define function and iterator to parallelize
run_in_parallel = lambda x: dbutils.notebook.run(x,timeout_seconds)
notebook_iterator = [notebook_name for i in range(num_threads)]

# Parallelize data gen
results = []
results.extend(pool.map(run_in_parallel,notebook_iterator))

# COMMAND ----------


