# Databricks notebook source
# MAGIC %md
# MAGIC # Telemetry Investigation⚠️
# MAGIC 1. Test whether the order of structfields affect the schema. 🧪
# MAGIC 2. Test whether the number of telemetry fields affect the schema. 🧪
# MAGIC 3. Test whether certain telemetry fields affect the schema. 🧪  
# MAGIC **Function**: investigate_telemetry()
# MAGIC ```
# MAGIC investigate_telemetry(DataFrame: DataFrame, Telemetry_Schema: list) -> DataFrame
# MAGIC ```
# MAGIC Note - This will take about an hour to run  

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports 🪄

# COMMAND ----------

from pyspark.sql.functions import StructType, when, isnull, isnotnull, lit, from_json, col
from pyspark.sql.types import StructField, StringType, IntegerType, StringType, BooleanType, DoubleType, FloatType
from pyspark.sql.dataframe import DataFrame
import numpy as np
import json

# COMMAND ----------

# MAGIC %md
# MAGIC # Widgets 🏷️

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Bronze"
    ,"operations.IoT.Bronze")
BRONZE_TABLE = dbutils.widgets.get('Bronze')
print(f'Bronze Table Name: {BRONZE_TABLE}')

# COMMAND ----------

# Set Device Type
dbutils.widgets.text(
    "DeviceType"
    ,"Compressors")
DEVICE_TYPE = dbutils.widgets.get('DeviceType')
print(f'Device Type: {DEVICE_TYPE}')

# COMMAND ----------

# MAGIC %run "./Device Templates/DeviceTemplateDictionary"

# COMMAND ----------

TEMPLATE_JSON

# COMMAND ----------

DEVICE_TEMPLATE = TEMPLATE_JSON[DEVICE_TYPE]
print(f'Device Template: {DEVICE_TEMPLATE}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
FLAG_DET_FOLDER_LOC = dbutils.widgets.get("External Location") + 'silver/flag_detail/'
print(f'Silver External Location: {FLAG_DET_FOLDER_LOC}')

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Silver Table"
    ,"operations.IoT.Silver")
FLAG_DETAIL_TABLE = dbutils.widgets.get('Silver Table') + '_flagdetail'
print(f'Flag Table Name: {FLAG_DETAIL_TABLE}')

# COMMAND ----------

def investigate_telemetry(DataFrame: DataFrame, Telemetry_Schema: list) -> DataFrame:
    '''
    Input: Dataframe, Telemetry_Schema, Schema
    Output: A Dataframe that lists telemetry affecting devices
    '''
    # Create an Empty DataFrame to Store Final Flags
    flags_df = spark.createDataFrame(data=[], schema=StructType([
    StructField('TemplateName', StringType(), True)
    ,StructField('DeviceId', StringType(), True)
    ,StructField('FlaggedField', StringType(), True)
    ]))

    # Assign Variables
    telemetry_list = Telemetry_Schema
    telemetry_columns = []
    np.random.shuffle(telemetry_list)
    MOD = True
    count = 1
    flags = []
    print(f'Starting total Telemetry Field Count: {len(telemetry_list)}')
    try:
        while MOD:
            for field in telemetry_list:
                count += 1 # Add to Counter for each iteration
                telemetry_columns += [field] # Add Telemetry Field to a New List
                print(f'Current Field: {field}')
                print(f'Current Column Count: {len(telemetry_columns)}')
                struct_template = StructType([
                    StructField('enqueuedTime', StringType(), True)
                    ,StructField('deviceId', StringType(),True)
                    ,StructField("templateId", StringType(), True)
                    ,StructField('templateName', StringType(), True)
                    ,StructField('telemetry'
                        ,StructType(telemetry_columns), True)
                    ])
                
                # Parse JSON
                df = (DataFrame
                        .withColumn('JSONTemplate', from_json(DataFrame['body'], schema=struct_template))
                    )
                df = df.withColumn('flagged'
                    ,when(
                        (isnull(df["JSONTemplate"]) & isnotnull(df["body"])) | isnull((df['JSONTemplate']['telemetry']))
                        , 1
                        )
                    .otherwise(0))
                flagged_df = df.filter(col('flagged') == 1)
                df_flagged_counts = flagged_df.count()
                if df_flagged_counts > 0:
                    [telemetry_list.remove(col) for col in telemetry_columns]
                    df_flagged = df.select('TemplateName','deviceId').distinct().withColumn('Field', lit(str(field)))
                    flags_df = flags_df.union(df_flagged)
                    print('\nFLAGS FOUND')
                    print(f'Current telemetry Column Count: {len(telemetry_columns)}')
                    print(f'Current field being processed: {field}')
                    print(f'Number of Rows flagged: {df_flagged_counts}\n')
                    telemetry_columns = []
                    count = 1
                    flags += [field]
                    print(f'New Count of Telemetry: {len(telemetry_list)}')
                    break
            if count >= len(telemetry_list):
                MOD = False
        return flags_df
    except Exception as e:
        print(e)

# COMMAND ----------

# Read Bronze Data
df = spark.sql(f'''
    SELECT 
        * 
    FROM {BRONZE_TABLE}
    WHERE enqueuedTime >= date_add(MINUTE, -60, enqueuedTime)
''')

# COMMAND ----------

import os
PATH = '/Workspace/Shared/dev-UAT/IOT Compressor Telemetry/Device Templates'
for path, subdirs, files in os.walk(PATH):
    for name in files:
        if name != '1. Note':
            print(name)

# COMMAND ----------

# IOT Central Exported Compressor Device Template
json_file = f'./Device Templates/{DEVICE_TEMPLATE}'

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

# Convert all string schema values to actual pyspark types
PYSPARK_DATATYPES = {
    'boolean': BooleanType()
    ,'float': FloatType()
    ,'double': DoubleType()
    ,'integer': IntegerType()
}

# COMMAND ----------

# Create all struct fields to put all headers
mappings = {}
for key, value in template_telemetry.items():
    mappings[key] = PYSPARK_DATATYPES[value]
    structfields = [StructField(header, formats, True) for header, formats in list(sorted(mappings.items()))]

# COMMAND ----------

# Grab template name from parsed JSON
template_name_struct = StructType([StructField('deviceId', StringType(),True),StructField('templateName', StringType(), True)])
df = (df
    .withColumn('TemplateName', from_json(df['body'], schema=template_name_struct)['templateName'])
    .withColumn('DeviceID', from_json(df['body'], schema=template_name_struct)['deviceId'])
)

# COMMAND ----------

# Filter for Device Type
df = df.filter(col('TemplateName') == DEVICE_TYPE)

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

df = (df
       .withColumn('JSONTemplate', from_json(df['body'], schema=template_struct))
    )

# COMMAND ----------

# Create logic to flag devices when JSON was not parsed correctly.
df = df.withColumn('flagged'
    ,when(
        (isnull(df["JSONTemplate"]) & isnotnull(df["body"])) | isnull((df['JSONTemplate']['telemetry']))
        , 1
        )
    .otherwise(0))

# COMMAND ----------

flagged_df = df.filter(col('flagged') == 1)

# COMMAND ----------

# flagged_df.count()
print('5/20/2024 - 30,538 row count for flags')

# COMMAND ----------

TEMP_TABLE = 'operations.iot.tempinvestigation'

# COMMAND ----------

spark.sql(f'''
   DROP TABLE IF EXISTS {TEMP_TABLE}       
''')

# COMMAND ----------

try:
    if flagged_df.count() > 0:
        print('Initiating save to temp table...')
        flagged_df.write.saveAsTable(TEMP_TABLE)
        print('Initiating read from temp table...')
        df = spark.sql(f'''SELECT * FROM {TEMP_TABLE}''')
        print('Initiating Investigation')
        inv_df = investigate_telemetry(DataFrame=df, Telemetry_Schema=structfields)
except Exception as e:
    raise e

# COMMAND ----------

# With a device template filter, can take up to 1 hour to process
display(inv_df.distinct())

# COMMAND ----------

FLAG_DET_FOLDER_LOC

# COMMAND ----------

FLAG_DETAIL_TABLE

# COMMAND ----------

try:
    inv_df.write.option('path', FLAG_DET_FOLDER_LOC).format('delta').mode('append').saveAsTable(FLAG_DETAIL_TABLE)
    spark.sql(f'''
        DROP TABLE IF EXISTS {TEMP_TABLE}       
    ''')
except Exception as e:
    raise e
