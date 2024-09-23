# Databricks notebook source
# MAGIC %md
# MAGIC # EventHub Pipeline 🪈
# MAGIC Bronze 🥉, Silver 🥈, Gold 🥇  
# MAGIC This notebook orchestrates the run of Bronze, Silver and Gold notebooks for ETL processes.
# MAGIC ### Notebooks
# MAGIC - 1. Event Hub Bronze Telemetry
# MAGIC - 2. Event Hub Silver Devices
# MAGIC - 3. Gold Devices

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets 🏷️

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set EventHub Time Interval ⚙️
# MAGIC - 1 hour 60
# MAGIC - 1 day 1,440
# MAGIC - 2 days 2,880
# MAGIC - 3 days 4,320
# MAGIC - 7 days 10,080

# COMMAND ----------

dbutils.widgets.text('EventHubMinutes', '3000')
minutes = int(dbutils.widgets.get("EventHubMinutes"))
print(f'EventHub Minutes: {minutes}')

# COMMAND ----------

# Set up a start time for batching
#  Bronze and Silver notebooks will utilize this to append new rows.
StartTime_dt = datetime.now() - timedelta(minutes=minutes)
StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
print(StartTime_str)

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Notebook Paths

# COMMAND ----------

# Assign Notebook paths
bronzenb = './1. Event Hub Bronze Telemetry'
silvernb = './2. Event Hub Silver Devices'

# COMMAND ----------

# Errors throw a WorkflowException.
# Create a function to retry running notebook if fails

def run_with_retry(notebook, timeout, args = {}, max_retries = 1):
  num_retries = 0
  while True:
    try:
      return dbutils.notebook.run(notebook, timeout, args)
    except Exception as e:
      if num_retries > max_retries:
        raise e
      else:
        print("Retrying error", e)
        num_retries += 1

# run_with_retry("LOCATION_OF_CALLEE_NOTEBOOK", 60, max_retries = 5)


# COMMAND ----------

# MAGIC %md
# MAGIC # Device Templates
# MAGIC A list of all current devices mapped to templates

# COMMAND ----------

# MAGIC %run "./Device Templates/DeviceTemplateDictionary"

# COMMAND ----------

# Show all available templates stored currently from DeviceTemplateDictionary notebook
DEVICES = TEMPLATE_JSON.keys()
print(DEVICES)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Notebook 🥉

# COMMAND ----------

# Run Bronze Notebook using specified start time
dbutils.notebook.run(path=bronzenb,timeout_seconds=14000, arguments={"EventHub StartTime": StartTime_str})

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Notebook 🥈 & Gold Notebooks 🥇

# COMMAND ----------

# Iterate through all current available device templates and create a silver table.
for asset in DEVICES:
    dbutils.notebook.run(silvernb, 14400, {"DeviceType":asset, "EventHub StartTime": StartTime_str})
    try:
        GOLD_PATH = f"./Gold Notebooks/Gold_{asset}"
        dbutils.notebook.run(GOLD_PATH, 14400)
    except Exception as error:
        if 'FAILED: Unable to access the notebook'.lower() in str(error).lower():
            print(f'Gold Notebook for {asset} not found')

# COMMAND ----------

# run_with_retry(bronzenb, 14400, {"EventHubMinutes": minutes})
# dbutils.notebook.run(silvernb, 1440, {"DeviceType": DEVICE_TYPE})
# # dbutils.notebook.run(goldnb, 1440)

# COMMAND ----------

print(f'Cluster Name: {spark.conf.get("spark.databricks.clusterUsageTags.clusterName")}')
print(f'Cluster NodeType: {spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")}')
print(f'Autotermination (mins): {spark.conf.get("spark.databricks.clusterUsageTags.autoTerminationMinutes")}')
print(f'Cluster Workers: {spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers")}')
