import pandas as pd
import logging
from tqdm import tqdm

from matrixmatcher import match_multiprocessing
from .mm_config import get_match_matrix_config


def get_match_potentials(df: pd.DataFrame) -> pd.DataFrame:
    """Determine duplicate potentials : both inter and intra data source"""
    zip_group = sorted(df.PLZ.str[:3].drop_duplicates().to_list())

    print(f"{str(len(zip_group))} zip groups have been created.")
    match_matrix, neighborhoods = get_match_matrix_config()

    df_list = []

    for group in tqdm(zip_group, desc="Processing groups"):
        print(group)
        df_slice = df.copy().loc[df.PLZ.str[:3] == group]

        matches = match_multiprocessing(
            df1=df_slice,
            df2=df_slice,
            matrix=match_matrix,
            neighborhoods=neighborhoods,
            disable_msgs=True,
            process_count=8,  # number of cores
        )
        df_result = matches.get_input_with_ids()

        if df_result['match_ID'].isna().all():
            df_result["match_ID"] = df_result["match_ID"].fillna(
            "unique_in_region"
        )
        else:
            df_result["match_ID"] = (group + "_" + df_result["match_ID"]).fillna(
            "unique_in_region"
        )

        df_list.append(df_result)

    return pd.concat(df_list, axis=0)
