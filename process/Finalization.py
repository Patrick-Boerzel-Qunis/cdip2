# Databricks notebook source
import sys
import dask
import dask.dataframe as dd
import numpy as np

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

LANDING_OUT_DIR = "data_pipeline/data_abraham_pipeline"
TARGET_TABLE = "t_finalization"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

aufb_path = f"az://landing/{LANDING_OUT_DIR}/t_entflechtung/*.parquet"
storage_options = {"account_name": account_name, "account_key": account_key}
df: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing

# COMMAND ----------

# AUR21: "Standartize Hausnummer"
df.Hausnummer = df["Hausnummer"].str.replace(" ", "").str.casefold()

# AUR22: "if Status = 0 then Master_Marketable = 0"
df["Master_Marketable"] = df["Master_Marketable"] & df["Status"]

# AAR06: "Adding dummy fields"
df["Flag_Blacklist"] = False
df["Flag_Customer"] = True

# AAR07: "Trasseninformationen Adding dummy fields"
# AAR08: "Adding dummy fields"
"""
Bestandskundenabgleich

Den Schlüssel für alle Informationen zu Bestandskunden stellt die Zentrale Versatel ID (ZVID) dar. Diese ID wird innerhalb des Master Data
Managements für jeden Eintrag vergeben, der im Bestandskunden-CRM Remedy oder im Vertriebs CRM MSD enthalten ist. Ausgehend von einer
bekannten ZVID lassen sich via Oracle DB beliebige weitere Detailinformationen ermitteln und anspielen.
Um zu ermitteln, ob ein CDIP Potential bereits mit ZVID im MDM gelistet ist, ist ein Adressabgleich erforderlich. Der DQ-Server stellt dafür einen erprobten
Workflow bereit, theoretisch kann der Abgleich aber über jeden Matching-Algorithmus erfolgen.
"""
for col in [
    "AB_CLUSTER",
    "AB_MUFFEN",
    "AB_TRASSEN_VERTRIEB",
    "AB_PLANTRASSEN_NICHT_WEBOM",
    "AB_PLANTRASSEN_NORMAL",
    "AB_PLANTRASSEN_5G",
    "AB_TRASSEN_ALL",
    "AB_TRASSEN_FREMD",
    "AB_TRASSEN_FREMD_BT",
    "AB_TRASSEN_FREMD_OHNE_BT",
    "AB_TRASSEN_OWN",
    "AB_TRASSEN_OWN_BT",
    "AB_TRASSEN_OWN_OHNE_BT",
    "PLZ_GEO",
    "ORT_GEO",
    "STRASSE_HNR_GEO",
    "X",
    "Y",
    # AAR08
    'Zentrale_Versatel_ID',
    'OMI_Firmenname',
    'OMI_Rechtsform',
    'OMI_Zahlen',
    'OMI_Service',
    'OMI_Verwaltung',
    'OMI_Holding',
    'OMI_Vertriebs', 
    'OMI_Beteiligungs',
    'OMI_Himmelsrichtung',
    'OMI_Grundstueck',
    'OMI_Bundesland',
    'OMI_Kirchengemeinde',
]:
    df[col] = None

for col in [
    "OWN_LR_PEC",
    "LR_TYP",
    "LR_DAT",
    "FREMD_LR_PEC",
    "AGS",
    "GEN",
    "BEZ",
    "CLUSTER_ID",
]:
    df[col] = ""


# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

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

# COMMAND ----------


