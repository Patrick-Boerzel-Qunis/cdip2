from typing import List, Any
import pandas as pd
import numpy as np
import networkx as nx


def deduplicate_companies_nationally(df: pd.DataFrame, node: str) -> pd.Series:
    """Deduplicate companies at National level using "node" and provide group ID."""
    df = df.reset_index()[
        [
            "PVID",
            "ENTFL_GROUP_ID",
            node,
        ]
    ].assign(ENTFL_GROUP_ID=lambda x: x.ENTFL_GROUP_ID.astype(str))

    # Get companies which needs to be deduplicated and group around the "node" attribute.
    df_small = _get_country_duplicate_group(df, node=node)
    dedup_groups = _get_dedup_groups(df_small, node=node)
    df_groups = pd.DataFrame(
        enumerate([list(g) for g in dedup_groups]),
        columns=["Group_Index", "ENTFL_GROUP_ID"],
    ).explode("ENTFL_GROUP_ID", ignore_index=True)

    df_groups = df_groups[
        df_groups["ENTFL_GROUP_ID"].str.startswith("group_id_")
    ].assign(ENTFL_GROUP_ID=lambda x: x["ENTFL_GROUP_ID"].str.partition("group_id_")[2])

    # Add "Group_Index" which maps the previous group in "ENTFL_GROUP_ID" to new grouping.
    # Drop duplicates in case of telephone number expansion. Note: As both telephone will have same PVID,
    # the resulting ENTFL_GROUP_ID will be same. Hence we can drop the duplicate at PVID level.
    df = df.drop_duplicates("PVID").merge(df_groups, how="left", on="ENTFL_GROUP_ID")

    # The dataframe is already sorted based on best company. Select the PVID of the top most company
    # in the group as the new "ENTFL_GROUP_ID".
    df_best = (
        df[~df["Group_Index"].isna()][["Group_Index", "PVID"]]
        .drop_duplicates(subset=["Group_Index"], keep="first")
        .rename(columns={"PVID": "ENTFL_GROUP_ID"})
    )

    # Merge and fIll standalone company with its own PVID.
    return (
        df[["PVID", "Group_Index"]]
        .merge(df_best, how="left", on="Group_Index")
        .assign(ENTFL_GROUP_ID=lambda x: x["ENTFL_GROUP_ID"].fillna(df["PVID"]))
        .set_index("PVID")
    )["ENTFL_GROUP_ID"]


def _get_country_duplicate_group(df: pd.DataFrame, node: str) -> pd.DataFrame:
    """Get companies in multiple location in the country."""
    df["group_count"] = (
        df.groupby(["ENTFL_GROUP_ID"])[["PVID"]].transform("count").fillna(0)
    )
    df["node_count"] = df.groupby([node])[["PVID"]].transform("count").fillna(0)

    return df[(df.group_count > 1) | (df.node_count > 1)].set_index("PVID")


def _get_dedup_groups(df: pd.DataFrame, node: str) -> List[Any]:
    """Create graph with ENTFL_GROUP_ID and "node" and provide the connected components."""
    df["entfl_group_graph"] = "group_id_" + df["ENTFL_GROUP_ID"].astype(str)
    # Networkx 3.1 does not allow None in the nodes.
    df[node] = np.where(df[node].isna(), "None", df[node])

    G = nx.Graph()
    G = nx.from_pandas_edgelist(df, "entfl_group_graph", node)

    # Connection over None is not valid. Hence delete the node.
    # Note : We cannot just drop the None values, as there are group of companies
    # without the "node" values and the group information will be deleted!
    if G.has_node("None"):
        G.remove_node("None")

    return list(nx.connected_components(G))
