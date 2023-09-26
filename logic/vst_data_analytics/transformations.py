from functools import reduce

from pyspark.sql import DataFrame


def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    def rename_column(df: DataFrame, col_definition: tuple[str, str]) -> DataFrame:
        return df.withColumnRenamed(col_definition[0], col_definition[1])

    return reduce(rename_column, mapping.items(), df)
