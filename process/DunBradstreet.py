# Databricks notebook source
import sys

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_id

# COMMAND ----------

sys.path.append(f"/Workspace/Repos/{user_id}/cdip-interim/logic")

# COMMAND ----------

from vst_data_analytics.mappings import COLUMN_DEFINITIONS, MAP_GENDER, MAP_TITLE
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
# MAGIC # Bisnode

# COMMAND ----------

# df_bisnode = read_data(spark, f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_*.parquet", COLUMN_DEFINITIONS["Bisnode"])
df_bisnode = read_data(
    spark,
    f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_0.parquet",
    COLUMN_DEFINITIONS["Bisnode"],
)
df_bisnode

# COMMAND ----------

df_bisnode = index_data(df_bisnode, "DUNS_Nummer")
df_bisnode

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode Primus

# COMMAND ----------

# df_bisnode_primus = read_data(spark, f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/02_bisnode_primus_2023_7_V.{version}_*.parquet", COLUMN_DEFINITIONS["BisnodePrimus"])
df_bisnode_primus = read_data(
    spark,
    f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/02_bisnode_primus_2023_7_V.{version}_5.parquet",
    COLUMN_DEFINITIONS["BisnodePrimus"],
)
df_bisnode_primus

# COMMAND ----------

df_bisnode_primus = index_data(df_bisnode_primus, "DUNS_Nummer")
df_bisnode_primus

# COMMAND ----------

# MAGIC %md
# MAGIC # Aufbereitung

# COMMAND ----------

df = join_data(df_bisnode, df_bisnode_primus)
df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nullwerte vereinheitlichen

# COMMAND ----------

df = replace_nan(df)
df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Akademische Titel standardisieren

# COMMAND ----------

df = AUR02_DnB(df, MAP_TITLE)
df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Anrede standardisieren

# COMMAND ----------

df = AUR03_DnB(df, MAP_GENDER)
df

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Egntl sollten hier _Nebenbranchen_ als _Listen fünfstelliger-Werte ausgewiesen werden_. Die Funktion ist aber eine noop -> wird hier weggelassen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Außerdem sollen hier _Beschaeftigte_ und _Segment_ als numerische Werte formatiert werden. Diese sind aber durch das Mapping bereits in den richtigen Type gecastet.

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
).saveAsTable("`vtl-dev`.landing.t_dnb")
