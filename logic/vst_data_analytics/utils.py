import dask.dataframe as dd


def any_true(a: dd.Series, b: dd.Series) -> dd.Series:
    return a | b


def row_exists(df: dd.DataFrame, col_name: str) -> dd.Series:
    return df[col_name].notnull()
