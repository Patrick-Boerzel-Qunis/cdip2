# Databricks notebook source
#dbutils.library.restartPython()

# COMMAND ----------

import sys
import pandas as pd
import numpy as np
from datetime import datetime

# COMMAND ----------

sys.path.append(f"../logic")

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

df_dnb = spark.read.table("`vtl-dev`.bronze.t_dnb").toPandas()
df_bed = spark.read.table("`vtl-dev`.bronze.t_bed").toPandas()

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

df_combined = pd.concat([df_dnb, df_bed])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Postprocessing after Combine

# COMMAND ----------

if "BED_Flag_Quality" in df_combined.columns and "Status" in df_combined.columns:
    df_combined = df_combined.assign(
        Master_Marketable=lambda x: ((x.Marketable == "Y") & (x.Status == "aktiv"))
        | (x.BED_Flag_Quality == "SELECT"),
    )
#TODO : The  following is dead code if df_combined has both DnB and BED data!  
elif "Status" in df_combined.columns:
    df_combined = df_combined.assign(
        Master_Marketable=lambda x: ((x.Marketable == "Y") & (x.Status == "aktiv")),
    )
    df_combined["BED_ID"] = np.NaN
else:
    df_combined = df_combined.assign(
        Master_Marketable=lambda x: (x.BED_Flag_Quality == "SELECT"),
    )
    df_combined["DUNS_Nummer"] = np.NaN

df_combined = df_combined.assign(
    Attribute_Count=lambda x: x[
        ["Firmenname", "Strasse", "Hausnummer", "PLZ", "Ort", "Telefon"]
    ].count(axis=1),
    Source=lambda x: np.where(x["DUNS_Nummer"].isna(), "BED", "DNB"),
)
if "GP_RAW_ID" not in df_combined.columns:
    #TODO : we need the definition to be : GP_RAW_ID=lambda x: np.where(x["DUNS_Nummer"].isna(), "bed_"+x.BED_ID.astype(str), "dnb_"+x.DUNS_Nummer.astype(str)),
    df_combined = df_combined.assign(
        GP_RAW_ID=range(0, df_combined.shape[0]), GP_RAW_ID_index=lambda x: x.GP_RAW_ID
    ).set_index("GP_RAW_ID_index")

now: str = str(datetime.now())
# Lückenfüller für die ominöse Username Env Variable, die vorher gesetzt wurde.
username: str = "dbx"
if "Last_Updated_By" not in df_combined.columns:
    df_combined["Active"] = "Y"
    df_combined["Last_Updated_By"] = "None"
    df_combined["Last_Update"] = "None"
    df_combined["Created_By"] = username
    df_combined["Created_Date"] = now

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform CombinedDataset

# COMMAND ----------

#TODO : new dataframe created. Use df/df_combined
df = AUR04(df_combined)  # complete telephone
df = AUR06(df) # telephone type
df = AUR07(df) # Bundesland capitalization
df = AUR10(df) # Umsatz to float --> check if necessary
df = AUR13(df) # Process Marketable, Firmenzentrale_Ausland, Tel_Select 
df = AUR14(df) # process Hauptbranche
df = AUR18(df) # process strase
df = AUR19(df) # process Telefon_complete --> contradicts AUR04
df = AUR20(df) # process Telefon_complete --> contradicts AUR04 & AUR19!
df = AUR21(df) # process Hausnummer
df = AUR108(df) # convert multiple title/position/gender text to single text
df = AUR109(df) # Status to boolean
df = AUR111(df)  # Process Handelsname , Is this needed, as in the first draft it was not included
df

# COMMAND ----------

df_final = spark.createDataFrame(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC *Titel_2* and *Title_3* can be of type *void*, so explicitly cast to string

# COMMAND ----------

# TODO : SRao : When the dataset is combined and after AUR108, we do not need the _1,_2,_3 data of DnB.
from pyspark.sql.types import StringType

possible_null_columns = {"Titel_2": StringType(), "Titel_3": StringType()}
for col_name, col_type in possible_null_columns.items():
    df_final = df_final.withColumn(col_name, df_final[col_name].cast(col_type))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Write Data

# COMMAND ----------

df_final.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(
    "`vtl-dev`.bronze.t_aufbereitung"
)
