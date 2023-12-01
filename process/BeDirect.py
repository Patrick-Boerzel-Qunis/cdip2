# Databricks notebook source
# dbutils.library.restartPython()

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


df: dd.DataFrame = read_data(
    path=bed_path,
    column_definitions=COLUMN_DEFINITIONS["BeDirect"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_*.parquet"
# bedirect_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bedirect_2023_7_V.{version}_0.parquet"
# df_raw = read_data(spark, bedirect_path, COLUMN_DEFINITIONS["BeDirect"])

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
df = AUR08(df, MAP_REV_MEDIAN)
df = AUR09(df, MAP_EMPL_MEDIAN)
df = AAR10(df, RECHTSREFORM_MAPPING)
df = AUR11(df) 
df = AUR12(df) 
df = AUR16(df)
df = AUR104(df)
df = AUR110(df) 
# df = index_data(df, "BED_ID")

# COMMAND ----------

#print(df.dtypes)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Map PLZ & Bundesland

# COMMAND ----------

plz_mapping_path = f"az://landing/data/plz_bundesland_mapping_2023_7_V.{version}_0.parquet"

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

df_bed_branch: dd.DataFrame = read_data(
    path=bed_branch_path,
    column_definitions=COLUMN_DEFINITIONS["MapBedBranche"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_*.parquet"
# bed_branch_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/bed_branch_mapping_2023_7_V.{version}_0.parquet"
# df_bed_branch = read_data(spark, bed_branch_path, COLUMN_DEFINITIONS["MapBedBranche"])
df_bed_branch = df_bed_branch.drop_duplicates()

# COMMAND ----------

df = merge_data(df, df_bed_branch)

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

#TODO : why Umsatz type gets converted to string?
df_copy = AAR051(df_copy, df_bisnode)

# COMMAND ----------

# MAGIC %md
# MAGIC Add Industry score

# COMMAND ----------

industrie_path = f"az://landing/data/industriescore_2023_7_V.{version}_0.parquet"

df_industrie: dd.DataFrame = read_data(
    path=industrie_path,
    column_definitions=COLUMN_DEFINITIONS["Industriescore"],
    account_name=account_name,
    account_key=account_key,
    engine="pyarrow",
)

# COMMAND ----------

# industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_*.parquet"
# industrie_path = f"abfss://landing@vtl0cdip0dev0std.dfs.core.windows.net/cdip_test/data/industriescore_2023_7_V.{version}_0.parquet"
# df_industrie = read_data(spark, industrie_path, COLUMN_DEFINITIONS["Industriescore"])

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

df_copy = df_copy[["BED_ID","Segment"]]

# COMMAND ----------

#df_copy.Segment.value_counts().compute()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join BeDirect & Segment Data

# COMMAND ----------

df = merge_data(df, df_copy, merge_on="BED_ID")

# COMMAND ----------

# df.compute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Data

# COMMAND ----------

#spark.createDataFrame(df).write.mode("overwrite").saveAsTable("`vtl-dev`.bronze.t_bed")

# COMMAND ----------

# A dask dataframe is made up of lazy partitions, but you are asking spark to send them to spark workers. This is not a simple operation! You could decide to create spark dataframes for each of the dask partitions, and concatenate them on the worker side - this would likely achieve what you are after but not be at all efficient.


# to_spark_df = dask.delayed(spark.createDataFrame)
# pieces = dask.compute(*[to_spark_df(d) for d in ddf.to_delayed()])
# spark_df = functoos.reduce(pyspark.sql.DataFrame.unionAll, pieces)

# COMMAND ----------

tmp_table = "BED_temp"

dd.to_parquet(df=df,
              path=f"az://landing/{tmp_table}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"

# COMMAND ----------

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("`vtl-dev`.bronze.t_bed")

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{tmp_table}"
dbutils.fs.rm(tmp_abfss_path, recurse=True)
