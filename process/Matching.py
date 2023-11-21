# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import dask
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from matching.match_potentials import get_match_potentials

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

df.dtypes

# COMMAND ----------

df_res = get_match_potentials(df)

# COMMAND ----------

ddf = dd.from_pandas(df_res, npartitions=13)

# COMMAND ----------

tmp_table = "mm_output"

dd.to_parquet(df=ddf,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )
