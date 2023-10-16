# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys

# COMMAND ----------

sys.path.append("/Workspace/Repos/libs/cdip-interim/logic")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

from vst_data_analytics.rules import address_master
from vst_data_analytics.transformations import merge_data
from vst_data_analytics.constants import ADDRESS_MASTER_URL, ADDRESS_MASTER_HEADERS

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Load data Aufbereitung

# COMMAND ----------

df_auf = spark.read.table("`vtl-dev`.bronze.t_aufbereitung").toPandas()
df_auf

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Prepare Address master

# COMMAND ----------

address_master_columns = {"Bundesland": "state", "Hausnummer": "streetNr", "Ort": "city", "PLZ": "postCode", "Strasse": "street", "GP_RAW_ID": "GP_RAW_ID"}
df_address_master = df_auf[[*address_master_columns.keys()]].rename(mapper=address_master_columns, axis="columns")
df_address_master

# COMMAND ----------

df_address_master = address_master(df=df_address_master, url=ADDRESS_MASTER_URL, headers=ADDRESS_MASTER_HEADERS)
df_address_master
