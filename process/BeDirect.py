# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
import dask
import dask.dataframe as dd

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

from vst_data_analytics.transformations import (
    replace_nan,
    index_data,
    merge_data,
    join_data,
)
from vst_data_analytics.preparation import read_data
from vst_data_analytics.mappings import (
    COLUMN_DEFINITIONS,
    MAP_TITLE,
    MAP_GENDER,
    MAP_REV_MEDIAN,
    MAP_EMPL_MEDIAN,
    RECHTSREFORM_MAPPING,
)
from vst_data_analytics.rules import (
    AUR02_BeD,
    AUR03_BeD,
    AUR08,
    AUR09,
    AUR11,
    AUR12,
    AUR16,
    AUR104,
    AUR110,
    AAR10,
    AAR050,
    AAR051,
    AAR053,
    AAR054,
    AAR055,
    AAR056,
    AAR057,
    AAR058,
    AAR059,
)

# COMMAND ----------

dask.config.set({"dataframe.convert-string": True})
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ----------

version = "00"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

bed_path = f"az://landing/data/bedirect_2023_7_V.{version}_*.parquet"

# COMMAND ----------

df: dd.DataFrame = read_data(
    path=bed_path,
    column_definitions=COLUMN_DEFINITIONS["BeDirect"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# # bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_*.parquet"
# bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_0.parquet"
# df_raw = read_data(spark, bedirect_path, COLUMN_DEFINITIONS["BeDirect"])

# COMMAND ----------

target_table = "`vtl-dev`.bronze.t_bed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform BeDirect

# COMMAND ----------

df = replace_nan(df)
df = AUR02_BeD(df, MAP_TITLE)
df = AUR03_BeD(df, MAP_GENDER)
# df = AUR08(df, MAP_REV_MEDIAN)
# df = AUR09(df, MAP_EMPL_MEDIAN)
df = AUR11(df) 
df = AUR12(df) 
df = AUR16(df)
df = AUR104(df)
df = AUR110(df) 
# df = index_data(df, "BED_ID")

# COMMAND ----------

# df.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Map PLZ & Bundesland

# COMMAND ----------

plz_mapping_path = f"az://landing/data/plz_bundesland_mapping_2023_7_V.{version}_0.parquet"

# COMMAND ----------

df_plz: dd.DataFrame = read_data(
    path=plz_mapping_path,
    column_definitions=COLUMN_DEFINITIONS["MapPlzBundesland"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# plz_mapping_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/plz_bundesland_mapping_2023_7_V.{version}_*.parquet"
# plz_mapping_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/plz_bundesland_mapping_2023_7_V.{version}_0.parquet"
# df_plz = read_data(spark, plz_mapping_path, COLUMN_DEFINITIONS["MapPlzBundesland"])
df_plz = df_plz.drop_duplicates(subset="PLZ")

# COMMAND ----------

df = merge_data(df, df_plz, merge_on="PLZ")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Map Branchentext

# COMMAND ----------

bed_branch_path = f"az://landing/data/bed_branch_mapping_2023_7_V.{version}_0.parquet"

# COMMAND ----------

df_bed_branch: dd.DataFrame = read_data(
    path=bed_branch_path,
    column_definitions=COLUMN_DEFINITIONS["MapBedBranche"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# # bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_*.parquet"
# bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_0.parquet"
# df_bed_branch = read_data(spark, bed_branch_path, COLUMN_DEFINITIONS["MapBedBranche"])
df_bed_branch = df_bed_branch.drop_duplicates()

# COMMAND ----------

df = merge_data(df, df_bed_branch)

# COMMAND ----------

df = AAR10(df, RECHTSREFORM_MAPPING)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calculate and Join BeD Segment value

# COMMAND ----------

# Select Columns & Copy Dataframe
df_copy = df[
    [
        "BED_ID",
        "Rechtsform",
        "Anzahl_Niederlassungen",
        "Firmenname",
        "Umsatz_Code",
        "Beschaeftigte_Code",
        "HNR",
        "Hauptbranche",
    ]
]

# COMMAND ----------

df_copy = AAR050(df_copy)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Merge Bisnode Data

# COMMAND ----------

bisnode_path = f"az://landing/data/01_bisnode_2023_7_V.{version}_*.parquet"

# COMMAND ----------

df_bisnode: dd.DataFrame = read_data(
    path=bisnode_path,
    column_definitions=COLUMN_DEFINITIONS["BisnodeForBeD"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# bisnode_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_*.parquet"
# bisnode_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_0.parquet"
# df_bisnode = read_data(spark, bisnode_path, COLUMN_DEFINITIONS["BisnodeForBeD"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## TCTEST Start

# COMMAND ----------

# df_copy.head(2)
# df_bisnode.head(2)

# COMMAND ----------

import numpy as np
import pandas as pd

def test(df_left: dd.DataFrame, df_right: dd.DataFrame) -> dd.DataFrame:
    _index_name: str = df_left.index.name
    _result: dd.DataFrame = None
    if _index_name is None:
        _result = df_left.merge(
            get_umsatz_score(df_right), how="left", on="Umsatz_Code"
        ).merge(get_beschaeftigte_score(df_right), how="left", on="Beschaeftigte_Code")
    else:
        _result = (
            df_left.reset_index()
            .merge(get_umsatz_score(df_right), how="left", on="Umsatz_Code")
            .merge(
                get_beschaeftigte_score(df_right), how="left", on="Beschaeftigte_Code"
            )
            .set_index(_index_name)
        )

    return _result

def get_umsatz_score(df_bisnode: dd.DataFrame) -> dd.DataFrame:
    df = (
        df_bisnode.replace("None", pd.NA)
        .assign(Umsatz=lambda x: x.Umsatz.astype("Float32"))
    )
    df["Umsatz_Score"] = df["Umsatz"].map_partitions(pd.cut, bins=[-np.inf, 10, 50, 250, 500, np.inf], labels=[1, 2, 3, 4, 5]).astype("Float32")
    df["Umsatz_Code"] = df["Umsatz"].map_partitions(pd.cut, bins=[-np.inf, 0.1, 0.25, 0.5, 2.5, 5, 25, 50, 500, np.inf], labels=["01", "02", "03", "04", "05", "06", "07", "08", "09"])
    g = df.groupby(["Umsatz_Code"])
    # Ziel: In jeder Staffel den gemittelten Durchschnitt von den entsprechenden Bisnode-Werten berechnen
    return dd.from_pandas(pd.DataFrame(
        {"Umsatz": g.Umsatz.mean(), "Umsatz_Score": g.Umsatz_Score.mean(), "Umsatz_Code": g.Umsatz_Code.first()}
    ), npartitions=2).set_index("Umsatz_Code")

def get_beschaeftigte_score(df_bisnode: dd.DataFrame) -> dd.DataFrame:
    df = (
        df_bisnode.replace("None",  pd.NA)
        .assign(Beschaeftigte=lambda x: x.Beschaeftigte.astype("Float32"))
    )
    df["Beschaeftigte_Score"] = df["Beschaeftigte"].map_partitions(pd.cut, bins=[-np.inf, 10, 50, 250, 999, np.inf], labels=[1, 2, 3, 4, 5]).astype("Float32")
    df["Beschaeftigte_Code"] = df["Beschaeftigte"].map_partitions(pd.cut, bins=[0, 4, 9, 19, 49, 99, 199, 499, 999, 1999, np.inf], labels=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"])
    g = df.groupby(["Beschaeftigte_Code"])
    # Ziel: In jeder Staffel den gemittelten Durchschnitt von den entsprechenden Bisnode-Werten berechnen
    return dd.from_pandas(pd.DataFrame(
        {
            "Beschaeftigte": g.Beschaeftigte.mean(),
            "Beschaeftigte_Score": g.Beschaeftigte_Score.mean(),
            "Beschaeftigte_Code": g.Beschaeftigte_Code.first(),
        }
    ), npartitions=2)




# COMMAND ----------

df_copy_test = test(df_copy, df_bisnode)
df_copy_test.head(10)

# COMMAND ----------

df_test_bis = df_bisnode.replace("None", np.NaN).assign(Umsatz=lambda x: x.Umsatz.astype(np.float32))

# COMMAND ----------

bins = [-np.inf, 10, 50, 250, 500, np.inf]
labels = [1, 2, 3, 4, 5]

# df_test_bis = df_test_bis.assign(Umsatz_Score='') 

def label_bins(partition):
    partition['Umsatz_Score'] = pd.cut(partition['Umsatz'], bins=bins, labels=labels, right=False)
    return partition[['Umsatz_Score']] 

df12 = df_test_bis.map_partitions(label_bins, meta={'Umsatz_Score': 'float'})
df12 = df12.compute()

print(df12.head(100))

# COMMAND ----------

df_test_bis.head()
# df12.head(100)

# COMMAND ----------

            Umsatz_Score=lambda x: pd.cut(
                x.Umsatz,
                bins=[-np.inf, 10, 50, 250, 500, np.inf],
                labels=[1, 2, 3, 4, 5],
            )

# COMMAND ----------

print(df_test_bis["Umsatz"].head(100))

# COMMAND ----------

def calculate_beschaeftigte_score(df: dd.DataFrame) -> dd.DataFrame:
    bins = [-np.inf, 10, 50, 250, 999, np.inf]
    labels = [1, 2, 3, 4, 5]

    def cut_beschaeftigte(partition):
        partition['Beschaeftigte_Score'] = pd.cut(partition['Beschaeftigte'], bins=bins, labels=labels).astype(np.float32)
        return partition

    return df.map_partitions(cut_beschaeftigte)

# COMMAND ----------


            Beschaeftigte_Score=lambda x: pd.cut(
                x.Beschaeftigte,
                bins=[-np.inf, 10, 50, 250, 999, np.inf],
                labels=[1, 2, 3, 4, 5],
            ).astype(np.float32)

# COMMAND ----------

df_copy = test(df_copy, df_bisnode)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TCTEST End

# COMMAND ----------

df_copy = AAR051(df_copy, df_bisnode) #Problem Patrick B solved it

# COMMAND ----------

industrie_path = f"az://landing/data/industriescore_2023_7_V.{version}_0.parquet"

# COMMAND ----------

# industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_*.parquet"
# industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_0.parquet"
# df_industrie = read_data(spark, industrie_path, COLUMN_DEFINITIONS["Industriescore"])

# COMMAND ----------

df_industrie: dd.DataFrame = read_data(
    path=industrie_path,
    column_definitions=COLUMN_DEFINITIONS["Industriescore"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# Previously implemented as AAR052, but it's a simple merge.
df_copy = merge_data(df_copy, df_industrie, merge_on="Hauptbranche")

# COMMAND ----------

df_copy = AAR053(df_copy)
df_copy = AAR054(df_copy)
df_copy = AAR055(df_copy)
df_copy = AAR056(df_copy)
df_copy = AAR057(df_copy)
df_copy = AAR058(df_copy)
df_copy = AAR059(df_copy)

# COMMAND ----------

df_copy = df_copy[["Segment"]]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join BeDirect & Segment Data

# COMMAND ----------

df = join_data(df, df_copy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Data

# COMMAND ----------

spark.createDataFrame(df).write.mode("overwrite").option(
    "overwriteSchema", "True"
).saveAsTable("`vtl-dev`.landing.t_bed")
