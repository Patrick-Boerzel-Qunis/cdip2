from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import DataType

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
