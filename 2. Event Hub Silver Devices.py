# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook 🥈
# MAGIC Reads Bronze data in batches for DeviceType from **parameter** and processes into Silver Table Structure.  
# MAGIC **Storage Account**: iotctsistorage  
# MAGIC **Container**: IOTeventhubtelemetry

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

# Imports
from datetime import datetime, timedelta
from pyspark.sql.functions import schema_of_json, lit, from_json, to_date, to_timestamp, max, min, when, isnull, isnotnull, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, FloatType, IntegerType
import json

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets 🏷️

# COMMAND ----------

# Set Device Type
dbutils.widgets.text(
    "DeviceType"
    ,"Compressors")
DEVICE_TYPE = dbutils.widgets.get('DeviceType')
print(f'Device Type: {DEVICE_TYPE}')

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Bronze"
    ,"operations.IoT.Bronze")
BRONZE_TABLE = dbutils.widgets.get('Bronze')
print(f'Bronze Table Name: {BRONZE_TABLE}')

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Silver Table"
    ,"operations.IoT.Silver")
SILVER_TABLE_DEVICE = dbutils.widgets.get('Silver Table') + f'_{DEVICE_TYPE.replace(" ","")}'
FLAG_TABLE = dbutils.widgets.get('Silver Table') + '_FLAGS'
print(f'Silver Table Name: {SILVER_TABLE_DEVICE}')
print(f'Flag Table Name: {FLAG_TABLE}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
SILVER_EXT_LOC =  dbutils.widgets.get("External Location") + 'silver'
SILVER_DEVICE_EXT_LOC = SILVER_EXT_LOC + f'/{DEVICE_TYPE}'
EXTERNAL_FLAG_LOC = SILVER_EXT_LOC + '/flags'
print(f'Silver External Location: {SILVER_EXT_LOC}')
print(f'Silver Device External Location: {SILVER_DEVICE_EXT_LOC}')
print(f'Silver Flag External Location: {EXTERNAL_FLAG_LOC}')

# COMMAND ----------

# Set event hub minute interval
dbutils.widgets.text('EventHub StartTime', 
    (datetime.now() - timedelta(minutes=60)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
)
START_TIME = dbutils.widgets.get("EventHub StartTime")
print(f'EventHub StartTime: {START_TIME}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Device Template JSON Parsing 🛠️

# COMMAND ----------

# MAGIC %run "./Device Templates/DeviceTemplateDictionary"

# COMMAND ----------

DEVICE_TEMPLATE = TEMPLATE_JSON[DEVICE_TYPE]
print(f'Device Template: {DEVICE_TEMPLATE}')

# COMMAND ----------

# Read Bronze Data
def incremental(default=True):
    if default:
        bronze_df = spark.sql(f'''
            SELECT 
                * 
            FROM {BRONZE_TABLE}
            WHERE enqueuedTime >= "{START_TIME}"
        ''')
    else:
        bronze_df = spark.sql(f'''
                SELECT 
                    * 
                FROM {BRONZE_TABLE}
        ''')
    return bronze_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## State Incremental or full push of silver table 🫸🏽
# MAGIC Either you preprocess batches of Bronze tables or all historical data from bronze

# COMMAND ----------

bronze_df = incremental()
# bronze_df = incremental(False)

# COMMAND ----------

# IOT Central Exported Compressor Device Template
json_file = f'./Device Templates/Devices/{DEVICE_TEMPLATE}'

with open(json_file) as f:
    data = json.load(f)

# COMMAND ----------

# Get all headers and schema values
template_telemetry = {}
for x in data:
    for key, value in x.items():
        for y in x['contents']:
            template_telemetry[y['name']] = y['schema']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert String Types to Pyspark Formats 🪛

# COMMAND ----------

# Convert all string schema values to actual pyspark types
PYSPARK_DATATYPES = {
    'boolean': BooleanType()
    ,'float': FloatType()
    ,'double': DoubleType()
    ,'integer': IntegerType()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a list of StructFields 📃
# MAGIC Creates a list of all telemetry points for the device

# COMMAND ----------

# Create all struct fields to put all headers
mappings = {}
for key, value in template_telemetry.items():
    mappings[key] = PYSPARK_DATATYPES[value]
    structfields = [StructField(header, formats, True) for header, formats in list(sorted(mappings.items()))]
# structfields[:5]

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply JSON Schema to DataFrame 📑

# COMMAND ----------

# Grab template name from parsed JSON
template_name_struct = StructType([StructField('deviceId', StringType(),True),StructField('templateName', StringType(), True)])
silver_df = (bronze_df
    .withColumn('TemplateName', from_json(bronze_df['body'], schema=template_name_struct)['templateName'])
    .withColumn('DeviceID', from_json(bronze_df['body'], schema=template_name_struct)['deviceId'])
)

# COMMAND ----------

# Filter for Device Type
silver_df = silver_df.filter(col('TemplateName') == DEVICE_TYPE)

# COMMAND ----------

# This is the main schema for all IOT Devices, what differs are the telemetry
template_struct = StructType([
    StructField('applicationId', StringType(), True)
    ,StructField('component', StringType(), True)
    ,StructField('enrichments', StringType(), True)
    ,StructField('messageProperties'
        ,StructType([StructField('iothub-creation-time-utc', StringType(), True)]), True)
    ,StructField('messageSource', StringType(), True)
    ,StructField('module', StringType(), True)
    ,StructField('schema', StringType(), True)
    ,StructField('templateId', StringType(), True)
    ,StructField('enqueuedTime', StringType(), True)
    ,StructField('deviceId', StringType(),True)
    ,StructField('templateName', StringType(), True)
    ,StructField('telemetry'
        ,StructType(
            structfields
        ), True)
])

# COMMAND ----------

silver_df = (silver_df
       .withColumn('JSONTemplate', from_json(silver_df['body'], schema=template_struct))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Check for flags 🔎

# COMMAND ----------

# Create logic to flag devices when JSON was not parsed correctly.
silver_df = silver_df.withColumn('flagged'
    ,when(
        (isnull(silver_df["JSONTemplate"]) & isnotnull(silver_df["body"])) | isnull((silver_df['JSONTemplate']['telemetry']))
        , 1
        )
    .otherwise(0))

# COMMAND ----------

# display(silver_df)

# COMMAND ----------

# Flags
df_flags = silver_df.filter(col('flagged') == 1).select('TemplateName','DeviceID', to_date('loaddatetime').alias('load_date')).distinct()

# COMMAND ----------

display(df_flags)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Flags to Table 📥

# COMMAND ----------

try:
    print(EXTERNAL_FLAG_LOC)
    print(FLAG_TABLE)
    if df_flags.count() > 0:
        (
            df_flags
            .write
            .option('path', EXTERNAL_FLAG_LOC)
            .format('delta')
            .mode('append')
            .saveAsTable(FLAG_TABLE)
        )
except Exception as e:
    raise e

# COMMAND ----------

silver_df = silver_df.filter(col('flagged') == 0)

# COMMAND ----------

# Expand JSON Template
silver_df = (
    silver_df
    .withColumn('Eventhub_enqueuedTime', to_timestamp(col('enqueuedTime')))
    .withColumn('Device_enqueuedTime', to_timestamp(col('JSONTemplate.enqueuedTime')))
    .withColumn('Device_Date', to_date(col('JSONTemplate.enqueuedTime')))
    .select('Eventhub_enqueuedTime','Device_enqueuedTime','Device_Date', 'templateName', 'DeviceId', 'JSONTemplate.telemetry.*','loaddatetime', 'rowhash')   
)
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Silver Table 📥

# COMMAND ----------

# Only write if the table does not exist
try:
    TABLE_EXISTS = spark.catalog.tableExists(SILVER_TABLE_DEVICE)
    if not TABLE_EXISTS:
        print(SILVER_DEVICE_EXT_LOC)
        print(SILVER_TABLE_DEVICE)
        (
            silver_df
            .write
            .option('path', SILVER_DEVICE_EXT_LOC)
            .format('delta')
            .mode('overwrite')
            .saveAsTable(SILVER_TABLE_DEVICE)
        )
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge Silver Table 📥

# COMMAND ----------

# Create a view for SQL statement
silver_df.createOrReplaceTempView('source')

# COMMAND ----------

# Set up variables for merge statement
target_cols =  silver_df.columns
insert_cols = ', '.join(target_cols)
insert_vals = ', '.join([f'source.{col}' for col in  target_cols])

# COMMAND ----------

# Create Merge Statement
merge_sql = f'''
MERGE INTO {SILVER_TABLE_DEVICE} AS target
USING source
    ON target.rowhash = source.rowhash
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals})
'''

# COMMAND ----------

try:
    display(spark.sql(merge_sql))
except Exception as e:
    raise e
