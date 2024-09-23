# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Notebook 🥉
# MAGIC This notebook will read Event Hub telemetry data and store it in [iotctsistorage](https://portal.azure.com/?pwa=1#@CLNE.onmicrosoft.com/asset/Microsoft_Azure_Storage/StorageAccount/subscriptions/3467f76c-b48c-40f0-90c9-c3b6429c0415/resourceGroups/IOTC-TSI/providers/Microsoft.Storage/storageAccounts/iotctsistorage) for a historical table.
# MAGIC ### Columns added to raw data
# MAGIC 1. **rowhash**: For deduplication when appending to storage account
# MAGIC 2. **loaddatetime**: To record when batch was ran.  
# MAGIC 3. **templateId**: To match with templateNames from IoT Central API  
# MAGIC 4. **templateName**: Show current device's template name.  
# MAGIC 5. **deviceId**: Show device id for row
# MAGIC 6. **deviceEnqueueTtime**: Get device datetime stamp
# MAGIC 7. **yearDevice, and monthDevice**: To partition data efficiently within storage.  
# MAGIC ### Tables Used
# MAGIC 1. **Operations.IoT.Bronze**: Stores all historical telemetry data
# MAGIC 2. **Operations.IoT.mergetemptable**: Table used to do merge process efficiently

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Requirements
# MAGIC **Library**  
# MAGIC com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql.functions import sha2, concat_ws, current_timestamp, col, from_json, year, month
from pyspark.sql.types import *
import json

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets 🏷️

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Bronze Table"
    ,"operations.IoT.Bronze")
BRONZE_TABLE_NAME = dbutils.widgets.get('Bronze Table')
print(f'Table Path: {BRONZE_TABLE_NAME}')

# COMMAND ----------

# Set event hub minute interval
dbutils.widgets.text('EventHub StartTime', 
    (datetime.now() - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
)
START_TIME = dbutils.widgets.get("EventHub StartTime")
print(f'EventHub StartTime: {START_TIME}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
BRONZE_EXT_LOC = dbutils.widgets.get("External Location") + 'bronze'
print(f'Bronze External Location: {BRONZE_EXT_LOC}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Eventhub Configuration ⚙️

# COMMAND ----------

# Connection string from Azure Key Vaults
connectionString = dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope','EventHub-Endpoint')

# COMMAND ----------

# Event Hubs configuration setup
ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
ehConf['eventhubs.consumerGroup'] = "$Default"
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

# 1 hour 60
# 1 day 1,440
# 2 days 2,880
# 3 days 4,320
# 7 days 10,080
# StartTime_dt = datetime.now() - timedelta(minutes=2000)
# StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
StartTime_str = START_TIME

EndTime_dt = datetime.now() - timedelta(minutes=0)
EndTime_str = EndTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

StartingEventPosition = {
  "offset": None,
  "seqNo": -1,            
  "enqueuedTime": StartTime_str,
  "isInclusive": True
}
EndingEventPosition = {
  "offset": None,           
  "seqNo": -1,
  "enqueuedTime": EndTime_str,
  "isInclusive": True
}

ehConf["eventhubs.startingPosition"] = json.dumps(StartingEventPosition)
ehConf["eventhubs.endingPosition"] = json.dumps(EndingEventPosition)

print(f'Start Time: {StartTime_str}\nEnd Time: {EndTime_str}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream Read / Write ETL 📥

# COMMAND ----------

# Read events from the Event Hub  specifically for template information
stream = (spark
    .read.format("eventhubs")
    .options(**ehConf).load()
    .withWatermark('enqueuedTime','1 minute')
)

# COMMAND ----------

bronze_schema = StructType([
    StructField('enqueuedTime', TimestampType(), True)
    ,StructField('deviceId', StringType(),True)
    ,StructField('templateName', StringType(), True)
    ,StructField('templateId', StringType(), True)
])

# COMMAND ----------

# Add a rowhash to create a composite key for the merge process and a loaddatetime stamp
stream = (stream
    .withColumn('body', col('body').cast('string'))
    .withColumn('templateId', 
        from_json(col('body')
            ,schema=bronze_schema
        )['templateId']
    )
    .withColumn('templateName', 
        from_json(col('body')
            ,schema=bronze_schema
        )['templateName']
    )
    .withColumn('deviceId', 
        from_json(col('body')
            ,schema=bronze_schema
        )['deviceId']
    )
    .withColumn('deviceEnqueueTtime', 
        from_json(col('body')
            ,schema=bronze_schema
        )['enqueuedTime']
    )
    .withColumn('yearDevice', year(col('deviceEnqueueTtime')))
    .withColumn('monthDevice', month(col('deviceEnqueueTtime')))
    .withColumn('rowhash', sha2(col('body'), 256))
    .withColumn('loadDatetime', current_timestamp())
)

# COMMAND ----------

# Temp Stream Table
TEMP_STREAM_TABLE = 'operations.iot.mergetemptable'

# COMMAND ----------

print(f'Cluster Name: {spark.conf.get("spark.databricks.clusterUsageTags.clusterName")}')
print(f'Cluster NodeType: {spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")}')
print(f'Autotermination (mins): {spark.conf.get("spark.databricks.clusterUsageTags.autoTerminationMinutes")}')
print(f'Cluster Workers: {spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers")}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Temp Table 📤

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note
# MAGIC 10080 mins or 7 days of data failed with:  
# MAGIC 14.3 LTS (includes Apache Spark 3.5.0, Scala 2.12)  
# MAGIC Standard_D16ads_v5 64 GB, 16 Cores  
# MAGIC 2 Workers 128 GB, 32 Cores  
# MAGIC
# MAGIC At best, its good to work with 3000 minutes or 2 days

# COMMAND ----------

try:
    (
        stream.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(TEMP_STREAM_TABLE)
    )
except Exception as e:
    raise e

# COMMAND ----------

# Ensure we create a table if it does not exist
try:
    TABLE_EXISTS = spark.catalog.tableExists(BRONZE_TABLE_NAME)
    if not TABLE_EXISTS:
        (
            stream
            .write
            .option('path',BRONZE_EXT_LOC)
            .format('delta')
            .mode('overwrite')
            .option('overwriteSchema', 'true')
            .saveAsTable(BRONZE_TABLE_NAME)
        )
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge Temp Table and Historical Table 🗃️

# COMMAND ----------

# Set up variables for MERGE Script
# Gets all columns and formats correctly for use

# Grab all columns from stored in catalog
target_col = stream.columns
# Convert columns to a string concatenated
insert_cols = ', '.join(target_col)
# create a format to get values from columns
insert_values = ', '.join([f"source.{col}" for col in target_col])
merge_sql = f"""
MERGE INTO {BRONZE_TABLE_NAME} AS target
USING {TEMP_STREAM_TABLE} AS source
    ON target.rowhash = source.rowhash
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_values})
"""

# COMMAND ----------

try:
    display(spark.sql(merge_sql))
except Exception as e:
    raise e

# COMMAND ----------

display(spark.sql(f'''
SELECT 
  MIN(enqueuedTime) AS MIN_Date
  ,MAX(enqueuedTime) AS MAX_Date
  ,DATEDIFF(minute, MIN(enqueuedTime), MAX(enqueuedTime)) AS StreamRange_Minutes
FROM {TEMP_STREAM_TABLE}
'''))

# COMMAND ----------

display(spark.sql(f'''DESCRIBE DETAIL {TEMP_STREAM_TABLE}''').select('sizeInBytes'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate Temp Table ✂️

# COMMAND ----------

spark.sql(f'''
TRUNCATE TABLE {TEMP_STREAM_TABLE}
''')
