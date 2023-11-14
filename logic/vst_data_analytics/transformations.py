import dask.dataframe as dd
import numpy as np
import pandas as pd


def rename_columns(df: dd.DataFrame, mapping: dict[str, str]) -> dd.DataFrame:
    return df.rename(columns=mapping)


def replace_nan(df: dd.DataFrame) -> dd.DataFrame:
    return df.replace(["None", "nan"], pd.NA)


def index_data(df: dd.DataFrame, name) -> dd.DataFrame:
    if df is None or name is None:
        return df

    _idx_name: str = df.index.name

    if name not in df.columns and name != _idx_name:
        return df

    _idx_name_copy: str = f"{name}_index"
    if f"{name}_index" == _idx_name:
        return df

    _model_data: dd.DataFrame = None
    if _idx_name is not None:
        _drop = _idx_name in df.columns or _idx_name_copy in df.columns
        _model_data = df.reset_index(drop=_drop)
    else:
        _model_data = df
    if _idx_name_copy not in _model_data.columns:
        _model_data[_idx_name_copy] = _model_data[name]
    _model_data = _model_data.set_index(_idx_name_copy)
    # TC: verify_integrity=True not available in dask
    return _model_data


def join_data(
    df: dd.DataFrame, df_other: dd.DataFrame, join_on: str = None
) -> dd.DataFrame:
    columns = df_other.columns.difference(df.columns)
    return df.join(df_other[columns], on=join_on, lsuffix="", rsuffix="_right")


def merge_data(
    df_left: pd.DataFrame, df_right: pd.DataFrame, merge_on: str = None
) -> pd.DataFrame:
    _index_name: str = df_left.index.name
    _index_name_no_copy: str = _index_name
    _result: pd.DataFrame = None

    if _index_name is None:
        _result = (
            df_left.merge(df_right, on=merge_on, how="left")
            if merge_on is not None
            else df_left.merge(df_right, how="left")
        )
    else:
        if _index_name_no_copy.endswith("_index"):
            _index_name_no_copy = _index_name_no_copy.replace("_index", "")

        if _index_name_no_copy not in df_left.columns:
            if _index_name != _index_name_no_copy:
                # create / save copy of index  which ends with _index  suffix
                df_left.reset_index(inplace=True)
                df_left[_index_name_no_copy] = df_left[_index_name]
                df_left.set_index(_index_name, inplace=True)
            else:
                # create / save copy of index  not ending with _index suffix
                df_left.reset_index(inplace=True)
                _index_name_copy = _index_name + "_index"
                df_left[_index_name_copy] = df_left[_index_name]
                df_left.set_index(_index_name_copy, inplace=True)
                _index_name = _index_name_copy

        _result = (
            df_left.merge(df_right, on=merge_on, how="left")
            if merge_on is not None
            else df_left.merge(df_right, how="left")
        )
        _result[_index_name] = _result[_index_name_no_copy]
        _result.set_index(_index_name, inplace=True)

    return _result
