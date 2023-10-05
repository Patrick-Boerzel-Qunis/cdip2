from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType

from vst_data_analytics.transformations import rename_columns

"""
Separate Spark logic for loading from business logic, which is written in pandas at
the moment.
"""


def cast_types(df: DataFrame, type_mappings: dict[str, DataType]):
    def cast_single(acc: DataFrame, col_definition: tuple[str, DataType]):
        col_name = col_definition[0]
        col_type = col_definition[1]
        return acc.withColumn(col_name, col(col_name).cast(col_type))

    return reduce(cast_single, type_mappings.items(), df)


def read_data(
    spark: SparkSession,
    path: str,
    column_definitions: dict[str, dict[str, str | DataType]],
) -> DataFrame:
    df = spark.read.format("parquet").load(path)
    columns = _get_old_columns(column_definitions)
    df = df.select(*columns)
    types = _get_column_types(column_definitions)
    df = cast_types(df, types)
    df = df.toPandas()
    column_mapping = _get_column_mapping(column_definitions)
    df = rename_columns(df, column_mapping)
    return df


def _get_old_columns(
    column_definitions: dict[str, dict[str, str | DataType]]
) -> list[str]:
    return [column for column in column_definitions.keys()]


def _get_column_types(
    column_definitions: dict[str, dict[str, str | DataType]]
) -> dict[str, DataType]:
    return {
        column_name: column_definition["type"]
        for column_name, column_definition in column_definitions.items()
    }


def _get_column_mapping(column_definitions: dict[str, dict[str, str | DataType]]):
    return {
        old_column_name: column_definition["name"]
        for old_column_name, column_definition in column_definitions.items()
    }
