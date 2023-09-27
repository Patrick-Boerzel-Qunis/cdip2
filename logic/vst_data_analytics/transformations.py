from functools import reduce

import pandas as pd
import numpy as np


def rename_columns(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.rename(columns=mapping)


def replace_nan(df: pd.DataFrame) -> pd.DataFrame:
    df.replace("None", np.NaN).replace("nan", np.NaN)

    return df
