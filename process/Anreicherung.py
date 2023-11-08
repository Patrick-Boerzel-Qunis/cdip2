# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_id

# COMMAND ----------

sys.path.append("/Workspace/Repos/gerd.first-gruettner@qunis.de/cdip-interim/logic")

# COMMAND ----------

import pandas as pd

# COMMAND ----------

from vst_data_analytics.rules import address_master, create_hauptbranche_id
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

# COMMAND ----------

df = merge_data(df_auf, df_address_master)
df = create_hauptbranche_id(df)
df


