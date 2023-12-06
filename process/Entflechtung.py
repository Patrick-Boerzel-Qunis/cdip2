# Databricks notebook source
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

LANDING_OUT_DIR = "data_pipeline"
TARGET_TABLE = "t_entflechtung"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

aufb_path = f"az://landing/{LANDING_OUT_DIR}/t_verflechtung/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_main: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

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

# Run Entflechtung algorithm
df = df.join(gp_entflechtung(df))

df.reset_index(inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

# COMMAND ----------

df_main = merge_data(df_main,df[["PVID","ENTFL_GROUP_ID", "ENTFL_NATIONAL", "ENTFL_ADDRESS", "Address_key"]],merge_on="PVID")

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{LANDING_OUT_DIR}/{TARGET_TABLE}"
dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------

dd.to_parquet(df=df_main,
              path=f"az://landing/{LANDING_OUT_DIR}/{TARGET_TABLE}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(f"`vtl-dev`.bronze.{TARGET_TABLE}")

# COMMAND ----------


