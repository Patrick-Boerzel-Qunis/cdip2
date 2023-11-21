import pandas as pd
import numpy as np


def sustain_only_best_company_data(df: pd.DataFrame) -> pd.DataFrame:
    """Nullify Telephone, Email and Address of non-best companies,so that they are not selected"""
    # Ensure that the variables selected in CONNECT_NODES_OF_BEST_COMPANIES are reset!
    df.loc[
        df.best_contact == 0,
        [
            "P1_TEL_KOMPLETT",
            "P2_TEL_KOMPLETT",
            "Email",
            "ASP_address_key",
        ],
    ] = (None, None, None, None)

    for col in [
        "P1_TEL_KOMPLETT",
        "P2_TEL_KOMPLETT",
        "Email",
        "ASP_address_key",
    ]:
        df[col] = df[col].replace({np.nan: None})

    return df


def process_results(df: pd.DataFrame) -> pd.DataFrame:
    """Derive the entflechtung categories of the companies

    ENTFL_NATIONAL = "Einzelunternehmen"
    --> A unique, stand-alone company

    ENTFL_NATIONAL = "Firmenzentrale"
    --> A company that is part of a group and the head of the group

    ENTFL_NATIONAL = "Niederlassung"
    --> A company that is part of a group and not the head

    ENTFL_ADDRESS = "Einzelunternehmen"
    --> A unique, stand-alone company

    ENTFL_ADDRESS = "Firmenzentrale"
    --> A company that is part of a group and the head of the group

    ENTFL_ADDRESS = "Nicht Bester am Standort"
    --> A company that is part of a group and not the best company of the group at the address

    ENTFL_ADDRESS = "Bester am Standort"
    --> A company that is part of a group and the best company of the group at the address

    ENTFL_ADDRESS = "Einziger am Standort"
    --> A company that is part of a group and the only company pf the group at the address

    """
    df = df.reset_index().assign(
        ENTFL_NATIONAL="",
        ENTFL_ADDRESS="",
        ENTFL_group_id_address=lambda x: x.ENTFL_GROUP_ID.astype(str)
        + "_"
        + x["Address_key"],
    )

    single_groups = (
        (df["ENTFL_GROUP_ID"].value_counts() == 1)
        .reset_index()
        .query("count == True")["ENTFL_GROUP_ID"]
    )

    single_groups_building = (
        (df.ENTFL_group_id_address.value_counts() == 1)
        .reset_index()
        .query("count == True")["ENTFL_group_id_address"]
    )

    # ENTFL_NATIONAL
    df.loc[
        df["ENTFL_GROUP_ID"].isin(single_groups), "ENTFL_NATIONAL"
    ] = "Einzelunternehmen"
    df.loc[df["best_contact"] == 0, "ENTFL_NATIONAL"] = "Niederlassung"
    df.loc[
        (df["best_contact"] == 1) & (~(df["ENTFL_GROUP_ID"].isin(single_groups))),
        "ENTFL_NATIONAL",
    ] = "Firmenzentrale"

    # ENTFL_ADDRESS
    df.loc[
        df["best_contact_building"] == 0, "ENTFL_ADDRESS"
    ] = "Nicht Bester am Standort"
    df.loc[
        (df["best_contact_building"] == 1)
        & (df["ENTFL_group_id_address"].isin(single_groups_building))
        & ~df["ENTFL_GROUP_ID"].isin(single_groups),
        "ENTFL_ADDRESS",
    ] = "Einziger am Standort"
    df.loc[
        (df["best_contact_building"] == 1)
        & (~(df["ENTFL_group_id_address"].isin(single_groups_building))),
        "ENTFL_ADDRESS",
    ] = "Bester am Standort"
    df.loc[
        df["ENTFL_GROUP_ID"].isin(single_groups), "ENTFL_ADDRESS"
    ] = "Einzelunternehmen"
    df.loc[
        (df["best_contact"] == 1) & (~(df["ENTFL_GROUP_ID"].isin(single_groups))),
        "ENTFL_ADDRESS",
    ] = "Firmenzentrale"

    # Maps the ENTFL_GROUP_ID
    group_id_to_pvid = dict(
        zip(
            df[df["ENTFL_NATIONAL"].isin(["Einzelunternehmen", "Firmenzentrale"])][
                "ENTFL_GROUP_ID"
            ],
            df[df["ENTFL_NATIONAL"].isin(["Einzelunternehmen", "Firmenzentrale"])][
                "PVID"
            ],
        )
    )
    df["ENTFL_GROUP_ID"] = df["ENTFL_GROUP_ID"].map(group_id_to_pvid)

    return df
