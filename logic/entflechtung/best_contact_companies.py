import numpy as np
import pandas as pd


def best_contact_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Mark the best company in a group as best contact at national and building level"""
    return (
        df.pipe(pre_processing_best_contact)
        .pipe(sort_best_contact_companies)
        .pipe(flag_best_contact_companies)
    )


def pre_processing_best_contact(df: pd.DataFrame) -> pd.DataFrame:
    """Derive flags that are needed for the best contact for each group determination."""
    df["Flag_Tele"] = np.where(
        df["P1_TEL_KOMPLETT"].notna() | df["P2_TEL_KOMPLETT"].notna(), 1, 0
    )
    df["Flag_Email"] = np.where(df["Email"].notna(), 1, 0)
    df["Flag_Website"] = np.where(df["Website"].notna(), 1, 0)
    df["Flag_Status"] = np.where(df["Status"], 1, 0)
    df["Flag_Marketable"] = np.where(df["Master_Marketable"], 1, 0)
    df["Flag_ASP_Name"] = np.where(df["Name"].notna(), 1, 0)

    return df


def sort_best_contact_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by descending order of the best potential contact company."""
    prio = [
        "ENTFL_GROUP_ID",
        "Flag_Status",
        "Flag_Marketable",
        "Flag_Tele",
        "Umsatz",
        "Flag_ASP_Name",
        "Flag_Website",
        "Flag_Email",
        "PVID",
    ]

    prio_ascending = [False, False, False, False, False, False, False, False, False]
    df = df.sort_values(by=prio, ascending=prio_ascending).drop(
        columns=[
            "Flag_Status",
            "Flag_Marketable",
            "Flag_Tele",
            "Flag_ASP_Name",
            "Flag_Website",
            "Flag_Email",
        ]
    )
    return df


def flag_best_contact_companies(df: pd.DataFrame) -> pd.DataFrame:
    """Mark the best contact for each group."""
    df["best_contact"] = 0
    best_indices = df.drop_duplicates("ENTFL_GROUP_ID", keep="first").index
    df.loc[df.index.isin(best_indices), "best_contact"] = 1

    df["best_contact_building"] = 0
    best_indices_building = df[
        (~df.duplicated(subset=["ENTFL_GROUP_ID", "Address_key"], keep="first"))
        | (df["Address_key"].isna())
    ].index
    df.loc[df.index.isin(best_indices_building), "best_contact_building"] = 1
    return df
