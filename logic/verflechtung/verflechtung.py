import pandas as pd
import networkx as nx
from typing import List, Any
import logging


def _add_hnr_grouping(df: pd.DataFrame) -> List[Any]:
    """Create graph with PVID of company and PVID of HNR company as nodes and provide the connected components."""
    G = nx.Graph()
    G = nx.from_pandas_edgelist(df, "PVID", "PVID_HNR")

    # Networkx module handles np.NaN correctly, but considers NoneType as node!
    if G.has_node(None):
        G.remove_node(None)

    hnr_groups = list(nx.connected_components(G))
    hnr_group_dict = [
        dict.fromkeys(pvid, group_id) for group_id, pvid in enumerate(hnr_groups)
    ]
    pvid_group_mapping = {
        pvid: group_id
        for hnr_group in hnr_group_dict
        for pvid, group_id in hnr_group.items()
    }

    df["HNR_Group"] = df["PVID_HNR"].map(pvid_group_mapping)
    return df


def _get_duplicate_groups(df: pd.DataFrame) -> pd.DataFrame:
    """Get companies in multiple matching group or part of larger group of company."""
    logging.info(
        f"Verflechtung : Number of standalone companies: {sum((df.PVID_HNR_count == 1) & (df.PVID == df.PVID_HNR))}"
    )
    return df[~((df.PVID_HNR_count == 1) & (df.PVID == df.PVID_HNR))]


def _select_pvid_hnr(df: pd.DataFrame) -> pd.DataFrame:
    """Select PVID of HNR company or PVID of a company among the group of company as the aggregated HNR."""
    df["HNR_Agg"] = (
        df.iloc[0]["PVID_HNR"]
        if not df.iloc[0]["HNR_not_present"]  # is HNR company present in data?
        else df.iloc[0]["PVID"]
    )

    return df


def get_aggregated_hnr(df_raw: pd.DataFrame) -> pd.Series:
    """
    Aggregate companies based on the PVID grouping and HNR grouping.

    Steps
        1. Create a graph of PVID of company and PVID of its HNR company.
        2. For each grouping extract the PVID of the HNR which is most occuring as HNR_Agg of the group.
            If the extracted PVID is "raw" HNR (HNR company not present in data),
            select the PVID as the HNR_Agg of the entire group.

    Parameters
    ----------
    df_raw : pd.DataFrame
        dataframe indexed at PVID, containing PVID_HNR,PVID_HNR_count and HNR_not_present.

    Returns
    -------
    HNR_Agg : PVID of the HNR to which the raw-potential belongs to.

    """
    logging.info(f"Verflechtung : Number of companies : {len(df_raw)}")

    df = df_raw[pd.notnull(df_raw.PVID_HNR)]
    logging.info(f"Verflechtung : Number of companies with HNR data: {len(df)}")

    df = df.join(
        (
            (
                df.assign(PVID_HNR=lambda x: x["PVID_HNR"].astype(int))
                .reset_index()
                .pipe(_get_duplicate_groups)
                .pipe(_add_hnr_grouping)
            )
            .sort_values(["HNR_not_present", "PVID_HNR_count"], ascending=[True, False])
            .groupby("HNR_Group")
            .apply(_select_pvid_hnr)
        )
        .set_index("PVID", verify_integrity=True)
        .HNR_Agg
    )

    return df.HNR_Agg.fillna(df.index.to_series()).astype(int)
