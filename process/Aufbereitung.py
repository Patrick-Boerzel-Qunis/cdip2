# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import pandas as pd
import dask.dataframe as dd
import numpy as np
from datetime import datetime

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

DEBUG = False

# COMMAND ----------

from vst_data_analytics.rules import (
    AUR04,
    AUR06,
    AUR07,
    AUR10,
    AUR13,
    AUR14,
    AUR18,
    AUR19,
    AUR20,
    AUR21,
    AUR108,
    AUR109,
    AUR111,
)

# COMMAND ----------

df_dnb = spark.read.table("`vtl-dev`.bronze.t_dnb").toPandas()
df_bed = spark.read.table("`vtl-dev`.bronze.t_bed").toPandas()

# COMMAND ----------

if DEBUG :
    print(df_dnb.shape)
    print(df_bed.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Dataset

# COMMAND ----------

DNB_UNIFIED_COLUMNS = {
    "Position_Text_1": "DNB_Position_Text_1",
    "Position_Text_2": "DNB_Position_Text_2",
    "Position_Text_3": "DNB_Position_Text_3",
    "Firmenzentrale_Ausland": "DNB_Firmenzentrale_Ausland",
    "Direkte_Mutter_Land": "DNB_Direkte_Mutter_Land",
    "Ehemaliger_Firmenname": "DNB_Ehemaliger_Firmenname",
    "Anzahl_Konzernmitglieder": "DNB_Anzahl_Konzernmitglieder",
    "Firmentype_Code": "DNB_Firmentype_Code",
}

BED_UNIFIED_COLUMNS = {
    "Anzahl_Toechter": "BED_Anzahl_Toechter",
    "Flag_Quality": "BED_Flag_Quality",
    "Tel_Select": "BED_Tel_Select",
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparation before Combine

# COMMAND ----------

df_dnb.rename(columns=DNB_UNIFIED_COLUMNS, inplace=True)
df_bed.rename(columns=BED_UNIFIED_COLUMNS, inplace=True)

# COMMAND ----------

# Assign DunBradstreet columns missing in BeDirect.
df_bed = df_bed.assign(
    Geschlecht_Text_1=lambda x: x.Geschlecht_Text,
    Geschlecht_Text_2=lambda x: np.NaN,
    Geschlecht_Text_3=lambda x: np.NaN,
    Titel_1=lambda x: x.Titel,
    Titel_2=lambda x: np.NaN,
    Titel_3=lambda x: np.NaN,
    Vorname_1=lambda x: x.Vorname,
    Vorname_2=lambda x: np.NaN,
    Vorname_3=lambda x: np.NaN,
    Name_1=lambda x: x.Name,
    Name_2=lambda x: np.NaN,
    Name_3=lambda x: np.NaN,
    HNR=lambda x: np.where(pd.isna(x.HNR), x.BED_ID, x.HNR).astype(int),
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine

# COMMAND ----------

df = pd.concat([df_dnb, df_bed])

del df_dnb,df_bed

# COMMAND ----------

if DEBUG:
    df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC #### Postprocessing after Combine

# COMMAND ----------

if "BED_Flag_Quality" in df.columns and "Status" in df.columns:
    df = df.assign(
        Master_Marketable=lambda x: ((x.Marketable == "Y") & (x.Status == "aktiv"))
        | (x.BED_Flag_Quality == "SELECT"),
    )

if "GP_RAW_ID" not in df.columns:
    #TODO : we need the definition to be : GP_RAW_ID=lambda x: np.where(x["DUNS_Nummer"].isna(), "bed_"+x.BED_ID.astype(str), "dnb_"+x.DUNS_Nummer.astype(str)),
    df = df.assign(
        GP_RAW_ID=range(0, df.shape[0])
    ).set_index("GP_RAW_ID")

now: str = str(datetime.now())
# Lückenfüller für die ominöse Username Env Variable, die vorher gesetzt wurde.
username: str = "dbx"
if "Last_Updated_By" not in df.columns:
    df["Active"] = "Y"
    df["Last_Updated_By"] = "None"
    df["Last_Update"] = "None"
    df["Created_By"] = username
    df["Created_Date"] = now

# COMMAND ----------

if DEBUG:
    df.shape

# COMMAND ----------

if DEBUG:
    df.columns

# COMMAND ----------

REQUIRED_COLUMNS =                 [
                    "Vorwahl_Telefon",
                    "Telefon",
                    "Bundesland",
                    "Umsatz",
                    "Marketable",
                    "Status",
                    "Hauptbranche",
                    "Handelsname",
                    "Ort",
                    "Strasse",
                    "Hausnummer",
                    'Geschlecht_Text', 
                    'Titel', 
                    'Vorname', 
                    'Name',
                    "Name_1",
                    "Name_2",
                    "Name_3",
                    "Vorname_1",
                    "Vorname_2",
                    "Vorname_3",
                    "Geschlecht_Text_1",
                    "Geschlecht_Text_2",
                    "Geschlecht_Text_3",
                    "DNB_Position_Text_1",
                    "DNB_Position_Text_2",
                    "DNB_Position_Text_3",
                    "Titel_1",
                    "Titel_2",
                    "Titel_3",
                ]

df_part = df[
                REQUIRED_COLUMNS
            ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform CombinedDataset

# COMMAND ----------

df_part= (df_part.pipe(AUR04) # complete telephone
          .pipe(AUR06)  # telephone type
          .pipe(AUR07)  # Bundesland capitalization
          .pipe(AUR10) # Umsatz to float --> check if necessary
          .pipe(AUR13) # Process Marketable, Firmenzentrale_Ausland, Tel_Select 
          .pipe(AUR14) # process Hauptbranche
          .pipe(AUR18) # process strasse
          .pipe(AUR19) # process Telefon_complete --> contradicts AUR04
          .pipe(AUR20) # process Telefon_complete --> contradicts AUR04 & AUR19!
          .pipe(AUR21) # process Hausnummer
          .pipe(AUR108) # convert multiple title/position/gender text to single text
          .pipe(AUR109) # Status to boolean
          .pipe(AUR111) # Process Handelsname ,@Patrick Is this needed, as in the first draft it was not included
          )

# COMMAND ----------

if DEBUG:
    df_part.head(5)

# COMMAND ----------

if DEBUG:
    df.shape

# COMMAND ----------

df = df.drop(columns=REQUIRED_COLUMNS).join(df_part).reset_index()
del df_part

# COMMAND ----------

if DEBUG:
    df.head(5)

# COMMAND ----------

tmp_table = "t_aufb"

df.to_parquet(path=f"az://landing/data/{tmp_table}/",
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

#tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"

# COMMAND ----------



# COMMAND ----------

#TODO :TC @patrick : Driver resource is not ussed, all spark df creation is done by worker, driver resources should be used
# df_final = spark.createDataFrame(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC *Titel_2* and *Title_3* can be of type *void*, so explicitly cast to string

# COMMAND ----------

# TODO  : SRao : When the dataset is combined and after AUR108, we do not need the _1,_2,_3 data of DnB.
from pyspark.sql.types import StringType

possible_null_columns = {"Titel_2": StringType(), "Titel_3": StringType()}
for col_name, col_type in possible_null_columns.items():
    df_final = df_final.withColumn(col_name, df_final[col_name].cast(col_type))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Write Data

# COMMAND ----------

#df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
#    "`vtl-dev`.bronze.t_aufbereitung"
#)

# COMMAND ----------

account_name = "cdip0dev0std"

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+ account_name +".dfs.core.windows.net",
dbutils.secrets.get(scope="cdip-scope", key="dask_key")
)

# COMMAND ----------

# Test if you can read the folder
dbutils.fs.ls("abfss://landing@cdip0dev0std.dfs.core.windows.net/")


# COMMAND ----------

tmp_table ="t_aufb"

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"

# COMMAND ----------

#df.write.format("parquet").mode("overwrite").save(tmp_abfss_path)

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

df.to_parquet(path=tmp_abfss_path,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------


