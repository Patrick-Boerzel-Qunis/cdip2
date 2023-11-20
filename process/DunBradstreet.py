# Databricks notebook source
import sys
import dask
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
from vst_data_analytics.transformations import index_data, join_data, replace_nan, merge_data
from vst_data_analytics.rules import AUR02_DnB, AUR03_DnB

# COMMAND ----------

dask.config.set({"dataframe.convert-string": True})
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

version = "00"

# COMMAND ----------

# MAGIC %md
# MAGIC Dask storage account  

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

target_table = "`vtl-dev`.bronze.t_dnb"

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode

# COMMAND ----------

bisnode_path = f"az://landing/data/01_bisnode_2023_7_V.{version}_*.parquet"

# COMMAND ----------

df_bisnode: dd.DataFrame = read_data(
    path=bisnode_path,
    column_definitions=COLUMN_DEFINITIONS["Bisnode"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

df_bisnode.head(2)

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
    engine="pyarrow",
)

# COMMAND ----------

df_bisnode_primus.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aufbereitung

# COMMAND ----------

df = merge_data(df_bisnode, df_bisnode_primus, merge_on = "DUNS_Nummer")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Nullwerte vereinheitlichen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Akademische Titel standardisieren

# COMMAND ----------

df = AUR02_DnB(df, MAP_TITLE)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Anrede standardisieren

# COMMAND ----------

df = AUR03_DnB(df, MAP_GENDER)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Egntl sollten hier _Nebenbranchen_ als _Listen fünfstelliger-Werte ausgewiesen werden_. Die Funktion ist aber eine noop -> wird hier weggelassen

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Außerdem sollen hier _Beschaeftigte_ und _Segment_ als numerische Werte formatiert werden. Diese sind aber durch das Mapping bereits in den richtigen Type gecastet.

# COMMAND ----------

tmp_table = "DnB_temp"

# COMMAND ----------

dd.to_parquet(df=df,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"

# COMMAND ----------

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(target_table)

# COMMAND ----------

dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------


