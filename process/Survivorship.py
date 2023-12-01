# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import dask
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from survivorship.golden_potentials import get_golden_potentials

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

aufb_path = f"az://landing/t_matching/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df_raw: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

df_raw = df_raw.compute()
df_raw.set_index("GP_RAW_ID",inplace=True)
#SRao : We can set the proper name in Anreicherung itself
df_raw = df_raw.rename(columns={'VT_addressId':'Address_ID', 'NACE-Code':'NACE_Code'})
df_raw

# COMMAND ----------

# load only required data
df_raw = df_raw[
    [
        "Firmenname",
        "DUNS_Nummer",
        "PLZ",
        "GKZ",
        "Segment",
        "Hauptbranchentext",
        "Nebenbranche",
        "Anzahl_Niederlassungen",
        "Beschaeftigte",
        "Rechtsform",
        "Register",
        "Website",
        "Email",
        "BED_ID",
        "Master_Marketable",
        "Bundesland",
        "Umsatz",
        "Status",
        "Hauptbranche",
        "Handelsname",
        "Ort",
        "Strasse",
        "Hausnummer",
        "Geschlecht_Text",
        "Titel",
        "Vorname",
        "Name",
        "Telefon_complete",
        "Telefon_Type",
        "Position_Text",
        "Address_ID",
        "NACE_Code",
        "Branche",
        "PVID",
        "PVID_HNR",
        "HNR_not_present",
        "PVID_count",
        "PVID_HNR_count",
        'Created_Date',
    ]
]

df_raw

# COMMAND ----------

df_gp = get_golden_potentials(df_raw)
df_gp

# COMMAND ----------

# reset index before writing to tables
df_gp = df_gp.reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

# COMMAND ----------

ddf = dd.from_pandas(df_gp, npartitions=10)

# COMMAND ----------

tmp_table = "t_survivorship"

dd.to_parquet(df=ddf,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )


# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_survivorship")

# COMMAND ----------

#tmp_table = "t_survivorship"
#tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
#dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------


