# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import dask
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from vst_data_analytics.transformations import merge_data
from verflechtung.verflechtung import get_aggregated_hnr

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

aufb_path = f"az://landing/t_survivorship/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_main: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

df_main.columns

# COMMAND ----------

df = df_main[["PVID","PVID_HNR","HNR_not_present", "PVID_HNR_count"]]
df= df.compute()
df.set_index("PVID",inplace=True)
df

# COMMAND ----------

df = df.join(get_aggregated_hnr(df))
df

# COMMAND ----------

df.reset_index(inplace=True)
df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

# COMMAND ----------

df_main = merge_data(df_main,df[["PVID","HNR_Agg"]],merge_on="PVID")
df_main

# COMMAND ----------

tmp_table = "t_verflechtung"

dd.to_parquet(df=df_main,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )


# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_verflechtung")

# COMMAND ----------


