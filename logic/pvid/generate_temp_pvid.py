import pandas as pd


def get_temp_pvid(df: pd.DataFrame) -> pd.DataFrame:
    """Provide temporary Potential Versatel ID to the matching group.

    Note : The time invariant PVID will be assigned in the stable PVID module.

    Parameters
    ----------
    df : pd.DataFrame
        Input data with match_ID, indexed at GP_RAW_ID

    Returns
    -------
    dataframe with PVID, indexed at GP_RAW_ID

    """
    df_group = df[df.match_ID != "unique_in_region"].assign(
        PVID=lambda x: x.groupby(["match_ID"]).ngroup()
    )

    last_pvid = max(df_group["PVID"])

    df_unique_company = df[df.match_ID == "unique_in_region"]
    df_unique_company["PVID"] = [
        last_pvid + 1 + i for i in range(len(df_unique_company))
    ]

    return pd.concat([df_group, df_unique_company], ignore_index=True)
