# Databricks notebook source
import sys

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_id

# COMMAND ----------

sys.path.append(f"/Workspace/Repos/{user_id}/cdip-interim/logic")

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

version = "00"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

# bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_*.parquet"
bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_0.parquet"
df_raw = read_data(spark, bedirect_path, COLUMN_DEFINITIONS["BeDirect"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform BeDirect

# COMMAND ----------

df = replace_nan(df_raw)
df = AUR02_BeD(df, MAP_TITLE)
df = AUR03_BeD(df, MAP_GENDER)
df = AUR08(df, MAP_REV_MEDIAN)
df = AUR09(df, MAP_EMPL_MEDIAN)
df = AUR11(df)
df = AUR12(df)
df = AUR16(df)
df = AUR104(df)
df = AUR110(df)
df = index_data(df, "BED_ID")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Map PLZ & Bundesland

# COMMAND ----------

# plz_mapping_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/plz_bundesland_mapping_2023_7_V.{version}_*.parquet"
plz_mapping_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/plz_bundesland_mapping_2023_7_V.{version}_0.parquet"
df_plz = read_data(spark, plz_mapping_path, COLUMN_DEFINITIONS["MapPlzBundesland"])
df_plz = df_plz.drop_duplicates(subset="PLZ")

# COMMAND ----------

df = merge_data(df, df_plz, merge_on="PLZ")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Map Branchentext

# COMMAND ----------

# bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_*.parquet"
bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_0.parquet"
df_bed_branch = read_data(spark, bed_branch_path, COLUMN_DEFINITIONS["MapBedBranche"])
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

# bisnode_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_*.parquet"
bisnode_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/01_bisnode_2023_7_V.{version}_0.parquet"
df_bisnode = read_data(spark, bisnode_path, COLUMN_DEFINITIONS["BisnodeForBeD"])

# COMMAND ----------

df_copy = AAR051(df_copy, df_bisnode)

# COMMAND ----------

# industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_*.parquet"
industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_0.parquet"
df_industrie = read_data(spark, industrie_path, COLUMN_DEFINITIONS["Industriescore"])

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
