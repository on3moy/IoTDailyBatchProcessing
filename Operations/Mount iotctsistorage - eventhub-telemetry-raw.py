# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Mounting Documentation
# MAGIC [DB Reference](https://docs.databricks.com/en/dbfs/mounts.html)
# MAGIC ## Azure Vault Documentation
# MAGIC [Adding Azure Vault to DB](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes)

# COMMAND ----------

# MAGIC %md
# MAGIC # Function created to resuse when mounting other data lake storages

# COMMAND ----------

def mountStorage(appId, directoryId, secret, container, storageAccount, mountName='data'):
    '''
    Function takes in SP information along with Data Lake storage information to mount into Databricks
    '''
    try:
        configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": appId,
            "fs.azure.account.oauth2.client.secret": secret,
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + directoryId + "/oauth2/token"}

        dbutils.fs.mount(
        source = "abfss://" + container + "@" + storageAccount +".dfs.core.windows.net/",
        mount_point = "/mnt/" + mountName,
        extra_configs = configs)
        print(f'Mounted: {mountName} using storage account: {storageAccount} and container: {container}')
        display(dbutils.fs.mounts())
    except Exception as e:
        print(e)

# COMMAND ----------

mountStorage(
    appId=dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope', 'sp-app-id'), 
    directoryId=dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope', 'sp-tenant-id'), 
    secret=dbutils.secrets.get('sgdataanalyticsdev-key-vault-scope', 'sp-secret'), 
    storageAccount='iotctsistorage', 
    container='eventhub-telemetry-raw', 
    mountName='eventhub-telemetry-raw', 
    )
