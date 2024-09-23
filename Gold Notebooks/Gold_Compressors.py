# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇Gold Compressor Notebook
# MAGIC Reads Silver data for compressors and processes into Gold Table Structure.  
# MAGIC **Storage Account**: iotctsistorage  
# MAGIC **Container**: IOTeventhubtelemetry

# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.types import FloatType
import datetime

# COMMAND ----------

# Set Device Type
dbutils.widgets.text(
    "DeviceType"
    ,"Compressors")
DEVICE_TYPE = dbutils.widgets.get('DeviceType')
print(f'Device Type: {DEVICE_TYPE}')

# COMMAND ----------

# Set managed bronze external location
dbutils.widgets.text('External Location', 'abfss://eventhub-telemetry@iotctsistorage.dfs.core.windows.net/')
GOLD_EXT_LOC =  dbutils.widgets.get("External Location") + 'gold'
GOLD_DEVICE_EXT_LOC = GOLD_EXT_LOC + f'/{DEVICE_TYPE}'
print(f'Gold External Location: {GOLD_EXT_LOC}')
print(f'Gold Device External Location: {GOLD_DEVICE_EXT_LOC}')

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Silver Table"
    ,"operations.IoT.Silver")
SILVER_TABLE_DEVICE = dbutils.widgets.get('Silver Table') + f'_{DEVICE_TYPE.replace(" ","")}'
FLAG_TABLE = dbutils.widgets.get('Silver Table') + '_FLAGS'
print(f'Silver Table Name: {SILVER_TABLE_DEVICE}')

# COMMAND ----------

# Set table name
dbutils.widgets.text(
    "Gold Table"
    ,"operations.IoT.Gold")
GOLD_TABLE_DEVICE = dbutils.widgets.get('Gold Table') + f'_{DEVICE_TYPE.replace(" ","")}'
print(f'Gold Table Name: {GOLD_TABLE_DEVICE}')

# COMMAND ----------

# Read Bronze Data
silver_df = spark.sql(f'SELECT * FROM {SILVER_TABLE_DEVICE}')

# COMMAND ----------

display(silver_df.limit(100))

# COMMAND ----------

# Get values we want to work with only
compressor_df = silver_df.select('Device_Date','deviceId','RUNNINGHR')

# COMMAND ----------

display(compressor_df.limit(100))

# COMMAND ----------

compressor_df = (compressor_df
    .withColumnRenamed('deviceId', 'deviceid')
    .withColumnRenamed('RUNNINGHR', 'runninghour')
    .withColumnRenamed('Device_Date','date')
)

# COMMAND ----------

# Telemetry data shown every 30 seconds from IOT Central
# display(compressor_df.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC # Count analysis
# MAGIC Lets conduct counts on below:
# MAGIC 1. nulls values.
# MAGIC 2. negative values.
# MAGIC 3. large values (anything over 100,000).
# MAGIC 4. zero values we get for the day.

# COMMAND ----------

TotalCount = compressor_df.groupBy('deviceid','date').count().withColumnRenamed('count','TotalCount')
# TotalCount.show()

# COMMAND ----------

NullCount = compressor_df.filter(compressor_df['runninghour'].isNull()).groupby('deviceid','date').count().withColumnRenamed('count','NullCount')
# NullCount.show()

# COMMAND ----------

NegCount = compressor_df.filter(compressor_df['runninghour'] < 0).groupby('deviceid','date').count().withColumnRenamed('count','NegCount')
# NegCount.show()

# COMMAND ----------

ZeroCount = compressor_df.filter(compressor_df['runninghour']  == 0).groupby('deviceid','date').count().withColumnRenamed('count','ZeroCount')
# ZeroCount.show()

# COMMAND ----------

LargeThreshold = 100000
LargeCount = compressor_df.filter(compressor_df['runninghour'] > LargeThreshold).groupby('deviceid','date').count().withColumnRenamed('count','LargeCount')
# LargeCount.show()

# COMMAND ----------

table_counts_df = (TotalCount
    .join(NegCount,on=(TotalCount['date'] == NegCount['date']) & (TotalCount['deviceid'] == NegCount['deviceid']), how='left').select(
        TotalCount['date']
        ,TotalCount['deviceid']
        ,NegCount['NegCount']
        ,TotalCount['TotalCount'])
    .join(LargeCount,on=(TotalCount['date'] == LargeCount['date']) & (TotalCount['deviceid'] == LargeCount['deviceid']), how='left').select(
        TotalCount['date']
        ,TotalCount['deviceid']
        ,NegCount['NegCount']
        ,LargeCount['LargeCount']
        ,TotalCount['TotalCount'])
    .join(ZeroCount,on=(TotalCount['date'] == ZeroCount['date']) & (TotalCount['deviceid'] == ZeroCount['deviceid']), how='left').select(
        TotalCount['date']
        ,TotalCount['deviceid']
        ,NegCount['NegCount']
        ,ZeroCount['ZeroCount']
        ,LargeCount['LargeCount']
        ,TotalCount['TotalCount'])
    .join(NullCount,on=(TotalCount['date'] == NullCount['date']) & (TotalCount['deviceid'] == NullCount['deviceid']), how='left').select(
        TotalCount['date']
        ,TotalCount['deviceid']
        ,NullCount['NullCount']
        ,NegCount['NegCount']
        ,ZeroCount['ZeroCount']
        ,LargeCount['LargeCount']
        ,TotalCount['TotalCount'])
 )
display(table_counts_df.select(TotalCount['date'],TotalCount['deviceid'],NegCount['NegCount'],ZeroCount['ZeroCount'], LargeCount['LargeCount'], NullCount['NullCount'], TotalCount['TotalCount']))

# COMMAND ----------

# table_counts_percent_df = (table_counts_df
#  .withColumn('NullPercent', table_counts_df['NullCount'] / table_counts_df['TotalCount'] * 100)
#  .withColumn('NegPercent', table_counts_df['NegCount'] / table_counts_df['TotalCount'] * 100)
#  .withColumn('ZeroPercent', table_counts_df['ZeroCount'] / table_counts_df['TotalCount'] * 100)
#  .withColumn('LargePercent', table_counts_df['LargeCount'] / table_counts_df['TotalCount'] * 100)
# ).select('date','deviceid','NullPercent','NegPercent', 'ZeroPercent','LargePercent','TotalCount')

# table_counts_percent_df.show()

# COMMAND ----------

# Adding Serverity Original Code
# def calculate_severity(deviceID, value_type, date_string, time_string, data_entry):
#     weights = {
#         "deviceID": 1,
#         "value_type": {
#             "normal": 0.0,
#             "large": 0.5,
#             "negative": 0.7,
#             "null": 0.3,
#             "zero": 0.3
#         },
#         "date_string": 1,
#         "time_string": 1,
#         "count": 0.7
#     }

#     severity_score = 0
#     for param, weight in weights.items():
#         if param == "date_string":
#             date_obj = datetime.strptime(date_string, '%B %d, %Y')  # Update the format here
#             severity_score += weight * (datetime.now() - date_obj).days
#         elif param == "time_string":
#             time_obj = datetime.strptime(time_string, '%H:%M:%S.%f')  # Update the format here
#             severity_score += weight * (time_obj.hour - 12) ** 2
#         elif param == "value_type":
#             severity_score += weight.get(value_type, 0.0)
#         else:
#             severity_score += weight * data_entry

#     return severity_score

# COMMAND ----------

# removing time string, cannot group with time to get day counts
def calculate_severity(deviceID, value_type, date_string, data_entry):
    weights = {
        "deviceID": 1,
        "value_type": {
            "normal": 0.0,
            "large": 0.5,
            "negative": 0.7,
            "null": 0.3,
            "zero": 0.3
        },
        "date_string": 1,
        # "time_string": 1,
        "count": 0.7
    }

    severity_score = 0
    for param, weight in weights.items():
        if param == "date_string":
            date_obj = date_string
            severity_score += weight * (datetime.date.today() - date_obj).days
        elif param == "value_type":
            severity_score += weight.get(value_type, 0.0)
        else:
            severity_score += weight * data_entry

    return severity_score

# COMMAND ----------

# Modified/Optimized Severity
# 100 is maximum severity
def severity(datevalue, value, counts):
    weights = {
        "large": 50
        ,"negative": 50
        ,"null": 30
        ,"zero": 40
        ,"normal": 0
        ,'sev0': 1
        ,'sev1': 1.5
        ,'sev2': 1.8
        ,'sev3': 2.0
    }

    # Number of days past severity multiplier
    sev1 = 3
    sev2 = 7
    sev3 = 14
    totalrows = 2880
    days = (datetime.date.today() - datevalue).days

    if days <= sev1:
        sev = weights['sev0'] * weights[value] * counts/totalrows
    elif days > sev1 and days <= sev2:
        sev =  weights['sev1'] * weights[value] * counts/totalrows
    elif days > sev2 and days <= sev3:
        sev =  weights['sev2'] * weights[value] * counts/totalrows
    elif  days > sev3:
        sev =  weights['sev3'] * weights[value] * counts/totalrows
    return round(sev,0)

# COMMAND ----------

severity_df = compressor_df.withColumn('valuetype'
    ,when(compressor_df['runninghour'] > LargeThreshold, 'large')
    .when(compressor_df['runninghour'] == 0, 'zero')
    .when(compressor_df['runninghour'] < 0, 'negative')
    .when(compressor_df['runninghour'].isNull(), 'null')
    .otherwise('normal')
)

# COMMAND ----------

# display(severity_df)

# COMMAND ----------

severity_grp = severity_df.groupBy(['date','deviceid', 'valuetype']).count().orderBy('date','deviceid')

# COMMAND ----------

# display(severity_grp)

# COMMAND ----------

calculate_severity_udf = udf(calculate_severity, FloatType())
results_df_with_severity = severity_grp.withColumn("severity"
    ,calculate_severity_udf(
        severity_grp["deviceID"]
        ,severity_grp["valuetype"]
        ,severity_grp["date"]
        ,severity_grp["count"]
    )
)

# COMMAND ----------

severity_udf_moy = udf(severity, FloatType())
results_df_with_severity = results_df_with_severity.withColumn("severity_moy"
    ,severity_udf_moy(
        severity_grp["date"]
        ,severity_grp["valuetype"]
        ,severity_grp["count"]
    )
)

# COMMAND ----------

# severity(datevalue=datetime.date(2024,4,1), value='negative', counts=2880)

# COMMAND ----------

# display(results_df_with_severity)

# COMMAND ----------

display(results_df_with_severity.groupby('date','deviceid').mean())

# COMMAND ----------

display(results_df_with_severity.describe().select('summary','severity_moy','severity'))

# COMMAND ----------

# Overwrite table
try:
    print(GOLD_DEVICE_EXT_LOC)
    print(GOLD_TABLE_DEVICE)
    (
        table_counts_df
        .write
        .option('path', GOLD_DEVICE_EXT_LOC)
        .format('delta')
        .mode('overwrite')
        .saveAsTable(GOLD_TABLE_DEVICE)
    )
except Exception as e:
    raise e

# COMMAND ----------

print(GOLD_EXT_LOC + '/' + DEVICE_TYPE +'_Severity')

# COMMAND ----------

# Overwrite table
try:
    GOLD_SEVERITY_EXT_LOC = GOLD_EXT_LOC + '/' + DEVICE_TYPE +'_Severity'
    GOLD_SEVERITY_TABLE_LOC = GOLD_TABLE_DEVICE + '_Severity'
    print(GOLD_SEVERITY_EXT_LOC)
    print(GOLD_SEVERITY_TABLE_LOC)
    (
        results_df_with_severity
        .write
        .option('path', GOLD_SEVERITY_EXT_LOC)
        .format('delta')
        .mode('overwrite')
        .saveAsTable(GOLD_SEVERITY_TABLE_LOC)
    )
except Exception as e:
    raise e

# COMMAND ----------

for table in spark.catalog.listTables('operations.iot'):
    print(table.name)
