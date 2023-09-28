from functools import reduce

import pandas as pd
import numpy as np


def rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=mapping)


def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace("None", np.NaN).replace("nan", np.NaN)


def index_data(df: pd.DataFrame, name) -> pd.DataFrame:
    if df is None or name is None:
        return df
    
    _idx_name: str = df.index.name
   
    if not name in df.columns and name != _idx_name:
        return df

    _idx_name_copy: str = f"{name}_index"
    if f"{name}_index" == _idx_name:
        return df

    _model_data: pd.DataFrame = None
    if _idx_name is not None:
        _drop = _idx_name in df.columns or _idx_name_copy in df.columns
        _model_data = df.reset_index(drop=_drop)
    else:
        _model_data = df
    if not _idx_name_copy in _model_data.columns:
        _model_data[_idx_name_copy] = _model_data[name]
    _model_data = _model_data.set_index(_idx_name_copy, verify_integrity=True)
    
    return [_model_data]
