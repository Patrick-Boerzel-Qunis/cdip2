# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

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

# MAGIC %md
# MAGIC
# MAGIC Load data Aufbereitung

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

# SRao : Why we load all table and persist when we can load only the required data and store them? We hav IDs anyway!
#df_auf = spark.read.table("`vtl-dev`.bronze.t_aufb").select(
#    'GP_RAW_ID',
#    'DUNS_Nummer',
#    'BED_ID',
#    'Bundesland',
#    'Hausnummer',
#    'Ort',
#    'PLZ',
#    'Strasse',
#    'Ort',
#    'Hauptbranche',
#).toPandas()
#df_auf

# COMMAND ----------

aufb_path = f"az://landing/t_aufb/*.parquet"
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
df_address_master

# COMMAND ----------

df_address_master = address_master(df=df_address_master, url=ADDRESS_MASTER_URL, headers=ADDRESS_MASTER_HEADERS)
df_address_master

# COMMAND ----------

# MAGIC %md
# MAGIC Save data for future

# COMMAND ----------

ddf = dd.from_pandas(df_address_master, npartitions=5)

# COMMAND ----------

tmp_table = "addres_master_output"

dd.to_parquet(df=ddf,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

df = merge_data(df_auf, df_address_master[["GP_RAW_ID","VT_addressId"]],merge_on = "GP_RAW_ID")

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add NACE data

# COMMAND ----------

df = create_hauptbranche_id(df)

# COMMAND ----------

data_path = f"az://landing/data/WZ_NACE_Mapping*.parquet"
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

df.head()

# COMMAND ----------

df = merge_data(df, df_nace,merge_on = "Hauptbranche_id").drop(columns=["Hauptbranche_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to table

# COMMAND ----------

tmp_table = "t_anreicherung"

dd.to_parquet(df=df,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )


# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_anreicherung")

# COMMAND ----------

dbutils.fs.rm(tmp_abfss_path, recurse=True)
