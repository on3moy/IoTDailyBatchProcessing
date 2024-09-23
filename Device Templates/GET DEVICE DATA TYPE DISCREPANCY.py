# Databricks notebook source
# MAGIC %md
# MAGIC # Device Templates 🗃️
# MAGIC ### Global Variable: TEMPLATE_JSON
# MAGIC  - These JSON files come from Device Templates stored in [IOT CENTRAL](https://ce-iot.azureiotcentral.com/device-templates)
# MAGIC  - Exported manually and imported into this workspace

# COMMAND ----------

import json

# COMMAND ----------

# For testing
from pyspark.sql.functions import StructType, from_json
from pyspark.sql.types import StructField, StringType

df = spark.sql('SELECT * FROM operations.iot.bronze')

# Grab template name from parsed JSON
template_name_struct = StructType([StructField('templateName', StringType(), True)])

df = (df
    .withColumn('TemplateName', from_json(df['body'], schema=template_name_struct)['templateName'])
)
df_templates = df.select('TemplateName').distinct()
display(df_templates.orderBy('TemplateName'))

# COMMAND ----------

# For testing
from pyspark.sql.functions import StructType, from_json
from pyspark.sql.types import StructField, StringType

df = spark.sql('SELECT * FROM operations.iot.bronze')

# Grab template name from parsed JSON
template_name_struct = StructType([StructField('templateName', StringType(), True)])

df = (df
    .withColumn('TemplateName', from_json(df['body'], schema=template_name_struct)['templateName'])
)
df.filter(df['TemplateName'] == '').display()

# COMMAND ----------

# Temporarily commented out other templates to not exceed azure quota.
TEMPLATE_JSON = {
    'Compressors':'./Devices/Compressors.json'
    # ,'LNG Tank 1': './Devices/LNG Tank.json'
    # ,'CNG Vaporizer 1': './Devices/CNG Vaporizer.json'
    # ,'PLC': './Devices/PLC.json'    
    # ,'LNG Dispenser 1': './Devices/LNG Dispenser.json'
    # ,'LNG Methane 1': './Devices/LNG Methane.json'
    # ,'LNG Pump 1': './Devices/LNG Pump.json'
    # ,'LNG Fire Eye 1': './Devices/LNG Fire Eye.json'
    ,'CNG Dispensers': './Devices/CNG Dispensers.json'
    ,'CNG Vapor Compressor': './Devices/CNG Vapor Compressor.json'
    ,'Dryers': './Devices/Dryers.json'
    ,'Dual Hose Dispenser': './Devices/Dual Hose Dispenser.json'
    ,'Site': './Devices/Site.json'
    ,'Transit Dispensers': './Devices/Transit Dispensers.json'
    # ,'Transit Dispensers - ANGI FF-100': './Devices/Transit Dispensers.json'
    ,'Time Fill': './Devices/Time Fill.json'
    ,'Priority Panel': './Devices/Priority Panel.json'
}

# COMMAND ----------

devices = TEMPLATE_JSON.keys()

# COMMAND ----------

stuff = []
for asset in devices:
    print(asset)
    # IOT Central Exported Compressor Device Template
    json_file = f'{TEMPLATE_JSON[asset]}'

    with open(json_file) as f:
        data = json.load(f)
        for x in data:
            for key, value in x.items():
                for y in x['contents']:
                    stuff += [[asset, y['name'], y['schema']]]

# COMMAND ----------

df = spark.createDataFrame(stuff, ['Asset', 'Telemetry', 'DataType']).distinct()
df.display()

# COMMAND ----------

group_df = df.groupBy('Telemetry', 'DataType').count()
group_df = group_df.groupBy('Telemetry').count()
group_df = group_df.filter(group_df['count'] > 1)
group_df.display()

# COMMAND ----------

group_df.join(df, on=['Telemetry'], how='left').select(['Telemetry', 'Asset', 'DataType']).display()
