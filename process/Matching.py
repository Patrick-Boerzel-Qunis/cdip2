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

from matching.match_potentials import get_match_potentials
from pvid.generate_temp_pvid import get_temp_pvid
from pvid.dereference_raw_id import raw_id_to_pvid

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

DEBUG = True

# COMMAND ----------

# MAGIC %md
# MAGIC Load data

# COMMAND ----------

aufb_path = f"az://landing/t_anreicherung/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_main: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
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

df

# COMMAND ----------

if DEBUG:
    sum(df.PLZ.isna())

# COMMAND ----------

# TODO: Convert the <NA> to None/numpy.NaN
# test by running on PLZ starting from '091'


# COMMAND ----------

if DEBUG:
    df.dtypes

# COMMAND ----------

# Matrix matcher needs the index to be properly set, but not any ID!
df.reset_index(drop=True, inplace=True)
df

# COMMAND ----------

NUM_CORES=16
df_res = get_match_potentials(df,NUM_CORES, thread_settings ={'threads': 2, 'slicesize': 10000},sliding_window_size=21)

# COMMAND ----------

ddf = dd.from_pandas(df_res, npartitions=13) # Almost 5h! the zip groups are done ca 13s each

# COMMAND ----------

tmp_table = "mm_output"

dd.to_parquet(df=ddf,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matching post processing

# COMMAND ----------

data_path = f"az://landing/mm_output/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_res: dd.DataFrame = dd.read_parquet(
    path=data_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

df_res = df_res.compute()
df_res

# COMMAND ----------

len(df_res.match_ID.unique())

# COMMAND ----------

sum(df_res.match_ID=="unique_in_region")

# COMMAND ----------

df_res = get_temp_pvid(df_res)
df_res.reset_index(inplace=True)
df_res

# COMMAND ----------

df_main = merge_data(df_main, df_res[["GP_RAW_ID","match_ID","PVID"]], merge_on="GP_RAW_ID")
df_main

# COMMAND ----------

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
df

# COMMAND ----------

df = raw_id_to_pvid(df)
df.reset_index(inplace=True)
df

# COMMAND ----------

df.columns

# COMMAND ----------

df_main = merge_data(df_main, df[["GP_RAW_ID",'PVID_HNR', 'HNR_not_present', 'PVID_count', 'PVID_HNR_count']], merge_on="GP_RAW_ID")
df_main

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write back to table
# MAGIC

# COMMAND ----------

tmp_table = "t_matching"

dd.to_parquet(df=df_main,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )


# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_matching")

# COMMAND ----------


