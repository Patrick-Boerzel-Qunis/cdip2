import pandas as pd
import logging

from .pre_processing import cleanse_attributes, create_asp_address_key
from .best_contact_companies import best_contact_companies
from .entflechtung_functions import sustain_only_best_company_data, process_results
from .create_network import deduplicate_companies_nationally

# Possible nodes : Telephone number(s),Email, ASP_address_key
CONNECT_NODES_OF_BEST_COMPANIES = {
    "P1_TEL_KOMPLETT": True,
    "TELE_COMPLETE": True,
    "Email": True,
    "ASP_address_key": True,
}


def gp_entflechtung(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Entflechtung of the golden potential data. Categorizes companies as

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

    Parameters
    ----------
    df : pd.DataFrame
        Golden potential data,after Verflechtung, indexed on PVID.

    Returns
    -------
    Golden potential data indexed on PVID and
    providing "ENTFL_GROUP_ID", "ENTFL_NATIONAL", "ENTFL_ADDRESS" and "Address_key" attributes.

    """
    df = df_raw.copy()
    logging.info(f"Entflechtung : Number of companies : {len(df)}")

    if df.index.name != "PVID":
        df.set_index("PVID", inplace=True, drop=True)

    df = (
        df.pipe(cleanse_attributes).pipe(
            create_asp_address_key,
        )
        # The HNR_Agg is the starting Entflectung group ID.
        .assign(ENTFL_GROUP_ID=lambda x: x.HNR_Agg)
    )

    logging.info(
        f"Entflechtung : Number of Entflechtung groups with HNR_Agg : {len(df.ENTFL_GROUP_ID.unique())}"
    )

    for conn_node, conn_on_best_comp in CONNECT_NODES_OF_BEST_COMPANIES.items():
        if conn_on_best_comp:
            df = df.pipe(best_contact_companies).pipe(
                sustain_only_best_company_data,
            )
        df_entfl_group = _get_entflechtung_groups(df, conn_node)

        logging.info(
            f"Entflechtung : Number of Entflechtung groups with {conn_node} : {len(df_entfl_group.unique())}"
        )

        # Update "ENTFL_GROUP_ID" based on the new grouping and drop old grouping.
        df = df.drop(columns="ENTFL_GROUP_ID").join(df_entfl_group)

    df = df.pipe(best_contact_companies).pipe(process_results).set_index("PVID")

    logging.info(
        f"Entflechtung : Number of Entflechtung groups final : {len(df.ENTFL_GROUP_ID.unique())}"
    )
    logging.info(
        f"Entflechtung : Value counts of ENTFL_NATIONAL : {df.ENTFL_NATIONAL.value_counts()}"
    )
    logging.info(
        f"Entflechtung : Value counts of ENTFL_ADDRESS : {df.ENTFL_ADDRESS.value_counts()}"
    )

    return df[["ENTFL_GROUP_ID", "ENTFL_NATIONAL", "ENTFL_ADDRESS", "Address_key"]]


def _get_entflechtung_groups(df: pd.DataFrame, conn_node: str) -> pd.DataFrame:
    """Provide entflechtung group by connecting companies based on the node"""
    if conn_node != "TELE_COMPLETE":
        return deduplicate_companies_nationally(df, node=conn_node)

    # Matching using both telephone numbers
    df_tel = pd.concat(
        [
            df[["ENTFL_GROUP_ID", "P1_TEL_KOMPLETT"]].rename(
                columns={"P1_TEL_KOMPLETT": "TELE_COMPLETE"}
            ),
            df[df["P2_TEL_KOMPLETT"].notna()][
                ["ENTFL_GROUP_ID", "P2_TEL_KOMPLETT"]
            ].rename(columns={"P2_TEL_KOMPLETT": "TELE_COMPLETE"}),
        ],
        ignore_index=False,
    )

    return deduplicate_companies_nationally(df_tel, node="TELE_COMPLETE")
