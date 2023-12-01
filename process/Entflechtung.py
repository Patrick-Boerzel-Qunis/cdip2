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
from entflechtung.main import gp_entflechtung

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

aufb_path = f"az://landing/t_verflechtung/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_main: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

df_main.columns

# COMMAND ----------

df = df_main[
    [
        "PVID",
        "HNR_Agg",
        "P1_TEL_KOMPLETT",
        "P2_TEL_KOMPLETT",
        "Email",
        "Website",
        "Status",
        "Master_Marketable",
        "Ort",
        "PLZ",
        "Strasse",
        "Hausnummer",
        "Vorname",
        "Name",
        'Umsatz',
    ]
]
df = df.compute()
df.set_index("PVID", inplace=True)
df

# COMMAND ----------

df = df.join(gp_entflechtung(df))
df

# COMMAND ----------

df.reset_index(inplace=True)
df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

# COMMAND ----------

df_main = merge_data(df_main,df[["PVID","ENTFL_GROUP_ID", "ENTFL_NATIONAL", "ENTFL_ADDRESS", "Address_key"]],merge_on="PVID")
df_main

# COMMAND ----------

tmp_table = "t_entflechtung"

dd.to_parquet(df=df_main,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )


# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_entflechtung")

# COMMAND ----------

#tmp_table = "t_entflechtung"
#tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
#dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------


