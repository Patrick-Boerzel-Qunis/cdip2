import dask.dataframe as dd
from vst_data_analytics.transformations import rename_columns


def sanitize_none(val: str) -> str:
    return val if val != "None" else None


def get_columns_of_type(type_mappings: dict[str, str], data_type: str) -> list[str]:
    return [key for key, value in type_mappings.items() if value == data_type]


def cast_types(df: dd.DataFrame, type_mappings: dict[str, str]) -> dd.DataFrame:
    int_columns = get_columns_of_type(type_mappings, "Int64")
    float_columns = get_columns_of_type(type_mappings, "Float64")
    number_columns = [*int_columns, *float_columns]
    df[number_columns] = df[number_columns].map(sanitize_none)
    return df.astype(type_mappings)


def read_data(
    path: str,
    column_definitions: dict[str, dict[str, str]],
    account_name: str,
    account_key: str,
) -> dd.DataFrame:
    storage_options = {"account_name": account_name, "account_key": account_key}
    df = dd.read_parquet(path, storage_options=storage_options)
    columns = get_old_columns(column_definitions)
    df = df[columns]
    types = get_column_types(column_definitions)
    df = cast_types(df, types)
    column_mapping = get_column_mapping(column_definitions)
    df = rename_columns(df, column_mapping)
    return df


def get_old_columns(column_definitions: dict[str, dict[str, str]]) -> list[str]:
    return list(column_definitions.keys())


def get_column_types(
    column_definitions: dict[str, dict[str, str]], type_key: str = "type"
) -> dict[str, str]:
    return {
        column_name: column_definition[type_key]
        for column_name, column_definition in column_definitions.items()
    }


def get_column_spark_types(
    column_definitions: dict[str, dict[str, str]]
) -> dict[str, str]:
    return get_column_types(
        column_definitions=column_definitions, type_key="spark_type"
    )


def get_column_mapping(column_definitions: dict[str, dict[str, str]]):
    return {
        old_column_name: column_definition["name"]
        for old_column_name, column_definition in column_definitions.items()
    }
