import pandas as pd
import numpy as np

from .labels import (
    GOLDEN_POTENTIAL_ATTRIBUTES,
    COMPANY_NAME_ATTRIBUTES,
    COMPANY_BRANCH_ATTRIBUTES,
    COMPANY_STATISTICS_ATTRIBUTES,
    ASP_ATTRIBUTES,
    COMPANY_ADDRESS_ATTRIBUTES,
    COMPANY_OTHER_ATTRIBUTES,
    HNR_ATTRIBUTES,
)


def _select_other_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Select the attributes from latest inserted company"""
    df = df.sort_values(
        ["Status", "Master_Marketable", "Source_Prio", "Created_Date"],
        ascending=[False, False, True, False],
    )
    g = df.groupby("PVID")
    return g[COMPANY_OTHER_ATTRIBUTES].first().join(g["Status"].all())


def _select_address_attributes_and_best_company(df: pd.DataFrame) -> pd.DataFrame:
    """Select the address attributes"""
    df = df.assign(
        Address_Attribute_Count=lambda x: x[COMPANY_ADDRESS_ATTRIBUTES].count(axis=1)
    )
    df = df.sort_values(
        [
            "Address_Attribute_Count",
            "Status",
            "Master_Marketable",
            "Source_Prio",
            "Created_Date",
        ],
        ascending=[False, False, False, True, False],
    )
    g = df.groupby("PVID")

    return (
        g[["PVID", *COMPANY_ADDRESS_ATTRIBUTES]].nth(0).set_index("PVID", drop=True)
    ).join(
        g[["DUNS_Nummer", "BED_ID"]]
        .first()
        .rename(columns={"DUNS_Nummer": "MAIN_DUNS_NUMMER", "BED_ID": "MAIN_BED_ID"})
    )


def _select_company_contact_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Select the company and contact attributes"""
    target_attributes = (
        COMPANY_NAME_ATTRIBUTES
        + COMPANY_BRANCH_ATTRIBUTES
        + COMPANY_STATISTICS_ATTRIBUTES
        + ASP_ATTRIBUTES
    )
    df = df.sort_values(
        ["Status", "Master_Marketable", "Source_Prio", "Created_Date"],
        ascending=[False, False, True, False],
    )

    return (
        df.groupby("PVID")[["PVID", *target_attributes]]
        .nth(0)
        .set_index("PVID", drop=True)
    )


def _select_tele_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Select the telephone attributes"""
    df["Tele_Prio"] = df["Telefon_Type"].map(
        {"Fixed network": 1, "Service": 2, "Mobile / Premium": 3}
    )

    df = df.sort_values(
        ["Tele_Prio", "Status", "Master_Marketable", "Source_Prio", "Created_Date"],
        ascending=[True, False, False, True, False],
    )

    g = (
        (
            df[df.Telefon_complete.notna()]
            .groupby(["PVID", "Telefon_complete"], sort=False)["Telefon_Type"]
            .first()
            .reset_index(level=1)
        )
        .groupby(level=0)
        .agg(list)
    ).join(
        (
            df[df.Telefon_complete.notna()]
            .groupby(["PVID", "Telefon_complete"], sort=False)["Master_Marketable"]
            .any()
            .reset_index(level=1)
            .drop(columns={"Telefon_complete"})
        )
        .groupby(level=0)
        .agg(list)
    )

    return (
        (
            g["Telefon_complete"]
            .apply(pd.Series)
            .add_prefix("tele_")[["tele_0", "tele_1"]]
            .rename(columns={"tele_0": "P1_TEL_KOMPLETT", "tele_1": "P2_TEL_KOMPLETT"})
        )
        .join(
            g["Telefon_Type"]
            .apply(pd.Series)
            .add_prefix("tele_typ_")[["tele_typ_0", "tele_typ_1"]]
            .rename(
                columns={
                    "tele_typ_0": "P1_TEL_TYP",
                    "tele_typ_1": "P2_TEL_TYP",
                }
            )
        )
        .join(
            g["Master_Marketable"]
            .apply(pd.Series)
            .add_prefix("marketable_")[["marketable_0", "marketable_1"]]
            .rename(
                columns={
                    "marketable_0": "P1_TEL_MARKETABLE",
                    "marketable_1": "P2_TEL_MARKETABLE",
                }
            )
        )
    )


def _select_hnr_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Select HNR company present in the data, and then latest company in DnB > BeD source."""
    df = df.assign(
        is_standalone_company=lambda x: (x.PVID_HNR_count == 1) & (x.PVID == x.PVID_HNR)
    ).sort_values(
        [
            "is_standalone_company",
            "Status",
            "Master_Marketable",
            "Source_Prio",
            "Created_Date",
        ],
        ascending=[True, False, False, True, False],
    )
    g = df.groupby("PVID")
    return g[["PVID", *HNR_ATTRIBUTES]].nth(0).set_index("PVID", drop=True)


def _apply_survivorship_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Apply survivorship rules to PVID group of companies"""
    df = df.assign(Source_Prio=lambda x: np.where(x.DUNS_Nummer.notna(), 2, 3))
    return (
        _select_other_attributes(df)
        .join(_select_address_attributes_and_best_company(df))
        .join(_select_tele_attributes(df))
        .join(_select_company_contact_attributes(df))
        .join(_select_hnr_attributes(df))
    )


def get_golden_potentials(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Application of survivorship rules to extract golden potentials

    Parameters
    ----------
    df : pd.DataFrame
        Raw potentials.

    Returns
    -------
    golden potential data.

    """
    df = df_raw.copy().reset_index(drop=True)

    # Filter PVID groups with only one company.
    df_unique = (
        df[df.PVID_count == 1]
        .set_index("PVID", drop=True)
        .rename(
            columns={
                "Telefon_complete": "P1_TEL_KOMPLETT",
                "Telefon_Type": "P1_TEL_TYP",
                "DUNS_Nummer": "MAIN_DUNS_NUMMER",
                "BED_ID": "MAIN_BED_ID",
            }
        )
        .assign(
            P2_TEL_KOMPLETT=np.NAN,
            P2_TEL_TYP=np.NAN,
            P1_TEL_MARKETABLE=lambda x: x.Master_Marketable,
            P2_TEL_MARKETABLE=np.NAN,
        )
    )[GOLDEN_POTENTIAL_ATTRIBUTES]

    return pd.concat(
        [df_unique, _apply_survivorship_rules(df[df.PVID_count > 1])],
        verify_integrity=True,
    )
