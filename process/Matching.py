# Databricks notebook source
import sys
import dask
import dask.dataframe as dd
import pandas as pd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from vst_data_analytics.transformations import merge_data

from matching.match_potentials import get_match_potentials
from pvid.generate_temp_pvid import get_temp_pvid
from pvid.dereference_raw_id import raw_id_to_pvid

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

LANDING_OUT_DIR = "data_abraham_pipeline"
TARGET_TABLE = "t_matching"

# COMMAND ----------

# MAGIC %md
# MAGIC Load data

# COMMAND ----------

data_path = f"az://landing/{LANDING_OUT_DIR}/t_anreicherung/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_main: dd.DataFrame = dd.read_parquet(
    path=data_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

df = df_main[
    [
        "Firmenname",
        "Handelsname",
        "PLZ",
        "Hausnummer",
        "Strasse",
        "Ort",
        "GP_RAW_ID",
    ]
]

df = df.compute()

# Matrix matcher needs the index to be properly set, but not any ID!
df.reset_index(drop=True, inplace=True)

# COMMAND ----------

# Convert the <NA> to None/numpy.NaN
for col in ['Firmenname','Handelsname','PLZ','Hausnummer','Strasse','Ort']:
    df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)

# COMMAND ----------

# Run matching algorithm
NUM_CORES=16
df_res = get_match_potentials(df,NUM_CORES,sliding_window_size=21, plz_digits = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save matching output

# COMMAND ----------

ddf = dd.from_pandas(df_res, npartitions=13) 

# COMMAND ----------

dd.to_parquet(df=ddf,
              path=f"az://landing/{LANDING_OUT_DIR}/mm_output/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching post processing

# COMMAND ----------

# Generate PVID for the match_IDs
data_path = f"az://landing/{LANDING_OUT_DIR}/mm_output/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_res: dd.DataFrame = dd.read_parquet(
    path=data_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

df_res = df_res.compute()
df_res = get_temp_pvid(df_res)
df_res.reset_index(inplace=True,drop=True)

# Add the PVID back to main data.
df_main = merge_data(df_main, df_res[["GP_RAW_ID","match_ID","PVID"]], merge_on="GP_RAW_ID")

# COMMAND ----------

# Convert HNR in terms of DnB/BED_ID to PVID
df = df_main[
    [
        "GP_RAW_ID",
        "DUNS_Nummer",
        "BED_ID",
        "PVID",
        "HNR",
    ]
]

df= df.compute()
df.set_index("GP_RAW_ID",inplace=True)
df = raw_id_to_pvid(df)
df.reset_index(inplace=True)

# Add the PVID_HNR and others back to main data.
df_main = merge_data(df_main, df[["GP_RAW_ID",'PVID_HNR', 'HNR_not_present', 'PVID_count', 'PVID_HNR_count']], merge_on="GP_RAW_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write back to table
# MAGIC

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


