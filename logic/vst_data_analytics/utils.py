import dask.dataframe as dd


def any_true(left: dd.Series, right: dd.Series) -> dd.Series:
    return left | right


def row_exists(df: dd.DataFrame, col_name: str) -> dd.Series:
    return df[col_name].notnull()
