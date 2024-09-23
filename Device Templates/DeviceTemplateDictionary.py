# Databricks notebook source
# MAGIC %md
# MAGIC # Device Templates 🗃️
# MAGIC ### Global Variable: TEMPLATE_JSON
# MAGIC  - These JSON files come from Device Templates stored in [IOT CENTRAL](https://ce-iot.azureiotcentral.com/device-templates)
# MAGIC  - Exported manually and imported into this workspace

# COMMAND ----------

# # Current list of all IOT Central Templates Active
# IOT_CENTRAL_DEVICE_TEMPLATE_LIST = [
#     'Power Meter'
#     ,'PLC'
#     ,'Matrix Panels'
#     ,'LNG Tank'
#     ,'LNG Site'
#     ,'LNG Pump'
#     ,'LNG Offload Pump'
#     ,'LNG Methane'
#     ,'LNG Fire Eye'
#     ,'LNG Dispenser'
#     ,'LCNG Pump v2'
#     ,'Dual Hose Dispenser'
#     ,'Dryers'
#     ,'Compressors'
#     ,'CNG Vaporizer'
#     ,'CNG Vapor Compressor'
#     ,'CNG Storage'
#     ,'CNG Skid'
#     ,'CNG Site'
#     ,'CNG Line'
#     ,'CNG Dispenser'
#     ,'CNG Booster'
#     ,'Transit Dispensers'
#     ,'Time Fill'
#     ,'Test REST API'
#     ,'Terminal Dispenser'
#     ,'Station Edge Gateway v5'
#     ,'Priority Panel'
# ]
# print(len(IOT_CENTRAL_DEVICE_TEMPLATE_LIST ))

# COMMAND ----------

# import json
# from datetime import datetime, timedelta
# # Set event hub minute interval
# StartTime_dt = datetime.now() - timedelta(minutes=60)
# StartTime_str = StartTime_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# # Event Hub is not reading encrypted connection string from Azure Key Vaults properly
# connectionString = dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope','EventHub-Endpoint')
# ehConf = {}
# ehConf['eventhubs.connectionString'] = connectionString
# ehConf['eventhubs.consumerGroup'] = "$Default"
# ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
# StartingEventPosition = {
#     "offset": None,
#     "seqNo": -1,            
#     "enqueuedTime": StartTime_str,
#     "isInclusive": True
# }
# ehConf["eventhubs.startingPosition"] = json.dumps(StartingEventPosition)
# stream = (spark
#     .readStream
#     .format("eventhubs")
#     .options(**ehConf).load()
#     .withWatermark('enqueuedTime','1 minute')
# )
# # Grab template name from parsed JSON
# template_name_struct = StructType([StructField('templateName', StringType(), True)])

# stream = (stream
#     .withColumn('TemplateName', from_json(stream['body'].cast(StringType()), schema=template_name_struct)['templateName'])
# )
# df_templates = stream.select('TemplateName').distinct()
# query = df_templates.writeStream.trigger(once=True).format("memory").queryName("distinct_templates").start()
# query.awaitTermination()

# distinct_templates = spark.sql('SELECT * FROM distinct_templates')
# templates = distinct_templates.collect()
# all_iot_templates = [row['TemplateName'] for row in templates]
# query.stop()

# all_iot_templates = [row['TemplateName'] for row in templates]
# print(len(all_iot_templates))

# for temp in IOT_CENTRAL_DEVICE_TEMPLATE_LIST:
#     if temp in all_iot_templates:
#         print(temp)

# COMMAND ----------

# # What is LNG system 1?
# for asset in all_iot_templates:
#     if asset not in IOT_CENTRAL_DEVICE_TEMPLATE_LIST:
#         print(asset)

# COMMAND ----------

# Temporarily commented out other templates to not exceed azure quota.
TEMPLATE_JSON = {
    'Compressors':'Compressors.json'
    ,'LNG Tank 1': 'LNG Tank.json'
    # ,'Dryers': 'Dryers.json'
    # , 'Terminal Dispenser': 'Terminal Dispensers.json'
    # , 'Matrix Panels': 'Matrix Panels.json'
    # , 'Skid': 'Skid.json'
    # , 'CNG Line': 'CNG Line.json'
    # , 'CNG Vapor Compressor': 'CNG Vapor Compressor.json'
    # , 'Transit Dispensers': 'Transit Dispensers.json'
    # , 'Priority Panel': 'Priority Panel.json'
    # , 'CNG Storage 1': 'CNG Storage.json'
    # , 'CNG Pump v2': 'LCNG Pump.json'
    # , 'LNG Offload Pump 1': 'LNG Offload Pump.json'
    # , 'CNG Vaporizer 1': 'CNG Vaporizer.json'
    # , 'Dual Hose Dispenser': 'Dual Hose Dispenser.json'
    # , 'CNG Booster Pump': 'CNG Booster.json'
    # , 'Time Fill': 'Time Fill.json'
    # , 'Site': 'Site.json'
    # , "PLC": 'PLC.json'    
    # , "LNG Dispenser 1": 'LNG Dispenser.json'
    # , "LNG Methane 1": 'LNG Methane.json'
    # , "LNG Pump 1": 'LNG Pump.json'
    # , "LNG Fire Eye 1": 'LNG Fire Eye.json'
    # , "CNG Dispensers": 'CNG Dispensers.json'
}
