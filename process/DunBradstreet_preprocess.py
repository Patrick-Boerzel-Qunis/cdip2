# Databricks notebook source
import sys
import dask
import dask.dataframe as dd

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

dask.config.set({"dataframe.convert-string": True})
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

LANDING_IN_DIR = "data_october"
TARGET_TABLE = "dnb_primus_preprocessed"

# COMMAND ----------

data_path = f"az://landing/{LANDING_IN_DIR}/02_bisnode_primus_2023_10_Vincent*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df = dd.read_parquet(data_path, storage_options=storage_options, engine="pyarrow")
df.columns

# COMMAND ----------

df= df.rename(columns={col: col.upper() for col in df.columns})
df.columns

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{LANDING_IN_DIR}/{TARGET_TABLE}"
dbutils.fs.rm(tmp_abfss_path, recurse=True)

dd.to_parquet(df=df,
              path=f"az://landing/{LANDING_IN_DIR}/{TARGET_TABLE}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )
              

# COMMAND ----------


