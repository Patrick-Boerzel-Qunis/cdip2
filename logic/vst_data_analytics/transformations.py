from functools import reduce

from pyspark.sql import DataFrame
import numpy as np

def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    def rename_column(df: DataFrame, col_definition: tuple[str, str]) -> DataFrame:
        return df.withColumnRenamed(col_definition[0], col_definition[1])

    return reduce(rename_column, mapping.items(), df)


def replace_nan(df: DataFrame) -> DataFrame:
    df.replace("None", np.NaN).replace("nan", np.NaN)

    return df
