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

# MAGIC %md
# MAGIC Load data

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

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
].set_index("GP_RAW_ID")

df = df.compute()

df

# COMMAND ----------

sum(df.PLZ.isna())

# COMMAND ----------

df.dtypes

# COMMAND ----------

df_tmp = df[df.PLZ.str[:3]=='091']

# COMMAND ----------

del df

# COMMAND ----------

df_tmp

# COMMAND ----------

NUM_CORES=8
df_res = get_match_potentials(df_tmp,NUM_CORES, thread_settings ={'threads': 2, 'slicesize': 10000},sliding_window_size=21)

# COMMAND ----------

df_res

# COMMAND ----------

df_tmp['PLZ_prefix'] = df_tmp.PLZ.str[:3]
df_slice = df_tmp.loc[df_tmp.PLZ_prefix == '091']
df_slice

# COMMAND ----------

from matrixmatcher import match_multiprocessing
from matching.mm_config import get_match_matrix_config

# COMMAND ----------

num_cores:int=8
thread_settings:dict = {'threads': 2, 'slicesize': 10000}
sliding_window_size:int=21
match_matrix, neighborhoods = get_match_matrix_config(sliding_window_size)

# COMMAND ----------

df_slice

# COMMAND ----------

df_slice.reset_index(inplace=True)
df_slice

# COMMAND ----------

df_slice.shape

# COMMAND ----------

#ddf = dd.from_pandas(df_slice, npartitions=1)
#tmp_table = "jannis_input"

#dd.to_parquet(df=ddf,
#              path=f"az://landing/{tmp_table}/",
#              write_index=False,
#              overwrite = True,
#              storage_options={'account_name': account_name,
#                               'account_key': account_key}
#              )

# COMMAND ----------

df_slice.set_index("GP_RAW_ID",inplace=True)
df_slice

# COMMAND ----------

matches = match_multiprocessing(
            df1=df_slice,
            df2=df_slice,
            matrix=match_matrix,
            neighborhoods=neighborhoods,
            disable_msgs=True,
            process_count=num_cores,
            threading_settings=thread_settings,
        )

# COMMAND ----------

df_result = matches.get_input_with_ids()
df_result

# COMMAND ----------

df_slice

# COMMAND ----------

df_slice.createOrReplaceTempView("v_diff")

# COMMAND ----------

df_result.shape

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

df_res.columns

# COMMAND ----------

df_main = df = merge_data(df_main, df_res, merge_on="GP_RAW_ID")

# COMMAND ----------

df_res = df_res.compute()
#df_res.index.name= "GP_RAW_ID"
df_res

# COMMAND ----------

df_res.index.value_counts()

# COMMAND ----------

df_main

# COMMAND ----------

df_res = get_temp_pvid(df_res)

# COMMAND ----------

df_res = raw_id_to_pvid(df_res)

# COMMAND ----------


