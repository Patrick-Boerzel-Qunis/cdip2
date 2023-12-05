# Databricks notebook source
import sys
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from vst_data_analytics.rules import address_master, create_hauptbranche_id
from vst_data_analytics.transformations import merge_data
from vst_data_analytics.constants import ADDRESS_MASTER_URL, ADDRESS_MASTER_HEADERS

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

LANDING_IN_DIR = "data_october"
LANDING_OUT_DIR = "data_pipeline"
TARGET_TABLE = "t_anreicherung"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Load data Aufbereitung

# COMMAND ----------

aufb_path = f"az://landing/{LANDING_OUT_DIR}/t_aufb/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_auf: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Prepare Address master

# COMMAND ----------

address_master_columns = {"Bundesland": "state", "Hausnummer": "streetNr", "Ort": "city", "PLZ": "postCode", "Strasse": "street", "GP_RAW_ID": "GP_RAW_ID"}
df_address_master = df_auf[[*address_master_columns.keys()]].rename(columns=address_master_columns)
df_address_master = df_address_master.compute()

# COMMAND ----------

df_address_master = address_master(df=df_address_master, url=ADDRESS_MASTER_URL, headers=ADDRESS_MASTER_HEADERS)

# COMMAND ----------

# MAGIC %md
# MAGIC Save data for future

# COMMAND ----------

ddf = dd.from_pandas(df_address_master, npartitions=5)

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{LANDING_OUT_DIR}/addres_master_output"
dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------

dd.to_parquet(df=ddf,
              path=f"az://landing/{LANDING_OUT_DIR}/addres_master_output/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

df = merge_data(df_auf, df_address_master[["GP_RAW_ID","VT_addressId"]],merge_on = "GP_RAW_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add NACE data

# COMMAND ----------

df = create_hauptbranche_id(df)

# COMMAND ----------

data_path = f"az://landing/{LANDING_IN_DIR}/WZ_NACE_Mapping*.parquet"
df_nace: dd.DataFrame = (
    dd.read_parquet(
        path=data_path,
        storage_options=storage_options,
        engine="pyarrow",
    )
    .rename(columns={"WZ_zwst": "Hauptbranche_id"})
    .drop_duplicates(subset="Hauptbranche_id")
)

# COMMAND ----------

df = merge_data(df, df_nace,merge_on = "Hauptbranche_id").drop(columns=["Hauptbranche_id"])

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
