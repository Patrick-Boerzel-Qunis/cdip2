# Databricks notebook source
#%pip install pandas==2.1.2

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

import sys
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from vst_data_analytics.mappings import (
    COLUMN_DEFINITIONS,
    MAP_GENDER,
    MAP_TITLE,
)
from vst_data_analytics.preparation import (
    read_data,
    cast_types,
    get_column_types,
    get_column_mapping,
)
from vst_data_analytics.transformations import index_data, join_data, replace_nan
from vst_data_analytics.rules import AUR02_DnB, AUR03_DnB

# COMMAND ----------

version = "00"

# COMMAND ----------

# MAGIC %md
# MAGIC Dask storage account  

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode

# COMMAND ----------

bisnode_path = f"az://landing/data/01_bisnode_2023_7_V.{version}_*.parquet"

# COMMAND ----------

df_bisnode = read_data(
    path=bisnode_path,
    column_definitions=COLUMN_DEFINITIONS["Bisnode"],
    account_name=account_name,
    account_key=account_key,
)
df_bisnode.head(20)

# COMMAND ----------

df_bisnode.sort_values("DUNS_Nummer")

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode Primus

# COMMAND ----------

bisnode_primus_path = f"az://landing/data/02_bisnode_primus_2023_7_V.{version}_*.parquet"

# COMMAND ----------

df_bisnode_primus = read_data(
    path=bisnode_primus_path,
    column_definitions=COLUMN_DEFINITIONS["BisnodePrimus"],
    account_name=account_name,
    account_key=account_key,
)
df_bisnode_primus.head(20)

# COMMAND ----------

df_bisnode_primus.sort_values("DUNS_Nummer")

# COMMAND ----------

df_bisnode_primus = index_data(df_bisnode_primus, "DUNS_Nummer")
df_bisnode_primus.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aufbereitung

# COMMAND ----------

df = join_data(df_bisnode, df_bisnode_primus)
df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nullwerte vereinheitlichen

# COMMAND ----------

df = replace_nan(df)
df.head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Akademische Titel standardisieren

# COMMAND ----------

# df = AUR02_DnB(df, MAP_TITLE)
# df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Anrede standardisieren

# COMMAND ----------

# df = AUR03_DnB(df, MAP_GENDER)
# df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Egntl sollten hier _Nebenbranchen_ als _Listen fünfstelliger-Werte ausgewiesen werden_. Die Funktion ist aber eine noop -> wird hier weggelassen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Außerdem sollen hier _Beschaeftigte_ und _Segment_ als numerische Werte formatiert werden. Diese sind aber durch das Mapping bereits in den richtigen Type gecastet.

# COMMAND ----------

dd.to_parquet(df=df,
              path='az://landing/temp/',
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

col_types = {
    **get_column_types(COLUMN_DEFINITIONS["Bisnode"]),
    **get_column_types(COLUMN_DEFINITIONS["BisnodePrimus"]),
}
col_mappings = {
    **get_column_mapping(COLUMN_DEFINITIONS["Bisnode"]),
    **get_column_mapping(COLUMN_DEFINITIONS["BisnodePrimus"]),
}
col_types = {
    col_mappings[col_name]: col_type for col_name, col_type in col_types.items()
}
cast_types(spark.createDataFrame(df), col_types).write.mode("overwrite").option(
    "overwriteSchema", "True"
).saveAsTable("`vtl-dev`.bronze.t_dnb")
#TC: catalog vtl-dev variablisieren

# COMMAND ----------

# MAGIC %md
# MAGIC TC-TEST
# MAGIC
# MAGIC 1. Versuchen dtypes from Dask laden. Implikationen Pandas Functions?
# MAGIC 2. Versuchen DnB und Bed Strecke mit Pyspark laden
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

df.head(20)

# COMMAND ----------

import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# COMMAND ----------

from vst_data_analytics.mappings2 import (
    COLUMN_DEFINITIONS2
)

# COMMAND ----------

def get_column_types2(column_definitions: dict[str, dict[str, str]]) -> dict[str, str]:
    return {
        column_name: column_definition["spark_type"]
        for column_name, column_definition in column_definitions.items()
    }

# COMMAND ----------

def get_column_mapping2(column_definitions: dict[str, dict[str, str]]):
    return {
        old_column_name: column_definition["name"]
        for old_column_name, column_definition in column_definitions.items()
    }


# COMMAND ----------

df.info(verbose=True)

# COMMAND ----------

col_types = {
    **get_column_types2(COLUMN_DEFINITIONS2["Bisnode2"]),
    **get_column_types2(COLUMN_DEFINITIONS2["BisnodePrimus2"]),
}
col_mappings = {
    **get_column_mapping2(COLUMN_DEFINITIONS2["Bisnode2"]),
    **get_column_mapping2(COLUMN_DEFINITIONS2["BisnodePrimus2"]),
}
col_types = {
    col_mappings[col_name]: col_type for col_name, col_type in col_types.items()
}
col_types

# COMMAND ----------

fields = [StructField(field, eval(spark_type)(), True) for field, spark_type in col_types.items()]
schema = StructType(fields)

sdf = spark.createDataFrame(df, schema=schema)

# COMMAND ----------

df_bisnode_primus.info()

# COMMAND ----------


