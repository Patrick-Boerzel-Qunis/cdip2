"""This script is scheduled after matching, generation of PVID and before the survivorship module.

The objective of this script is to map the HNR numbers which refer to DUNS_Nummer/BED_ID to PVIDs.

"""
import logging

import numpy as np
import pandas as pd

ID_OFFSET = 500000000000


def raw_id_to_pvid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the raw id references in HNR

    Parameters
    ----------
    df : pd.DataFrame
        dataframe indexed at GP_RAW_ID, containing PVID,DUNS_Nummer, BED_ID and HNR.

    Returns
    -------
    df : pd.DataFrame
        dataframe indexed at GP_RAW_ID, containing PVID_HNR,HNR_not_present, PVID_count and PVID_HNR_count.

    """
    df = df.assign(
        PVID=lambda x: x["PVID"].astype(int),
        raw_id=lambda x: np.where(
            x.DUNS_Nummer.notna(), x.DUNS_Nummer, x.BED_ID
        ).astype(int),
    )

    id_hnr_mapping = dict(zip(df.raw_id, df.PVID))

    df_w_hnr = (
        df[pd.notnull(df.HNR)].assign(
            HNR=lambda x: x["HNR"].astype(int),
            PVID_HNR=lambda x: x["HNR"].map(id_hnr_mapping),
            # Explicitly mark the HNR companies not found in the data.
            HNR_not_present=lambda x: np.where(x.PVID_HNR.isna(), True, False),
        )
        # In cases where the HNR company is not present in the data, we still need
        # the information for grouping.
        # Add offset so that the HNR space and PVID space does not collide.
        .assign(PVID_HNR=lambda x: x.PVID_HNR.fillna(x.HNR + ID_OFFSET).astype(int))
    )

    logging.info(
        f"Survivorship : Number of HNR companies not present: {sum(df_w_hnr.HNR_not_present)}"
    )

    df = df.join(df_w_hnr[["PVID_HNR", "HNR_not_present"]]).assign(
        PVID_count=lambda x: x.PVID.map(x["PVID"].value_counts()),
        PVID_HNR_count=lambda x: x.PVID_HNR.map(x["PVID_HNR"].value_counts()),
    )

    return df
