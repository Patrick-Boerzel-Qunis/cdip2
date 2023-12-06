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

# MAGIC %md
# MAGIC Dask storage account  

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

LANDING_IN_DIR = "data_october"
LANDING_OUT_DIR = "data_pipeline"
TARGET_TABLE = "t_dnb"

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode

# COMMAND ----------

bisnode_path = f"az://landing/{LANDING_IN_DIR}/01_bisnode_*.parquet"

# COMMAND ----------

df_bisnode: dd.DataFrame = read_data(
    path=bisnode_path,
    column_definitions=COLUMN_DEFINITIONS["Bisnode"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bisnode Primus

# COMMAND ----------

bisnode_primus_path = f"az://landing/{LANDING_IN_DIR}/dnb_primus_preprocessed/*.parquet"

# COMMAND ----------

df_bisnode_primus = read_data(
    path=bisnode_primus_path,
    column_definitions=COLUMN_DEFINITIONS["BisnodePrimus"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final DnB data

# COMMAND ----------

df = merge_data(df_bisnode, df_bisnode_primus, merge_on = "DUNS_Nummer")

# COMMAND ----------

df = AUR02_DnB(df, MAP_TITLE) # academic titel standardization
df = AUR03_DnB(df, MAP_GENDER) # Anrede standardisieren

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to table

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{LANDING_OUT_DIR}/{TARGET_TABLE}"
dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------

dd.to_parquet(df=df,
              path=f"az://landing/{LANDING_OUT_DIR}/{TARGET_TABLE}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(f"`vtl-dev`.bronze.{TARGET_TABLE}")

# COMMAND ----------


