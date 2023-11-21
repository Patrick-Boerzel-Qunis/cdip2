import pandas as pd
from typing import Union


def cleanse_attributes(df: pd.DataFrame) -> pd.DataFrame:
    """Filter blank strings from various attributes."""
    for col in ["P1_TEL_KOMPLETT", "P2_TEL_KOMPLETT"]:
        df[col] = df[col].str.replace(" ", "")
        df.loc[df[col] == "", col] = None
    return df


def create_asp_address_key(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Create string of complete address and complete address with name."""
    df["Address_key"] = _create_address_key(df, ["Ort", "PLZ", "Strasse", "Hausnummer"])
    df["ASP_address_key"] = _create_address_key(df, ["Address_key", "Vorname", "Name"])

    df.loc[
        (df.ASP_address_key == "nan")
        | (df["Vorname"].isna())
        | (df["Name"].isna())
        | (df["Ort"].isna())
        | (df["PLZ"].isna())
        | (df["Strasse"].isna()),
        "ASP_address_key",
    ] = None
    df.loc[
        (df.Address_key == "nan")
        | (df["Ort"].isna())
        | (df["PLZ"].isna())
        | (df["Strasse"].isna()),
        "Address_key",
    ] = None

    return df


def _create_address_key(
    df: pd.DataFrame,
    column_in: Union[str, list[str]],
    name: str = "address_key",
) -> pd.Series:
    """Bilde einen Hilfsstring um Adressabgleiche durchzuführen.

    Die Funktion ist primär für die Anwendung von Strings des folgenden
    Formates ausgelegt:
        'PLZ Ort Straßenname Hausnummer'

    """
    if isinstance(column_in, str):
        column_in = [column_in]
    if not column_in:
        message = "Argument 'column_in' darf nicht Länge 0 haben"
        raise ValueError(message)
    se = df[column_in[0]].fillna("").astype(str)
    for c in column_in[1:]:
        se += " " + df[c].fillna("").astype(str)

    se = (
        se.str.casefold()
        .str.replace("ä", "ae", regex=False)
        .str.replace("ö", "oe", regex=False)
        .str.replace("ü", "ue", regex=False)
        .str.replace("[", "(", regex=False)
        .str.replace("]", ")", regex=False)
        .str.replace("{", "(", regex=False)
        .str.replace("}", ")", regex=False)
        .str.replace(r"\(.*\)", "", regex=True)
        .str.replace(r"\W+", "_", regex=True)
        .str.replace(r"(\d+)([a-z]+)", "\g<1>_\g<2>", regex=True)  # noqa: W605
        .str.replace(r"([a-z]+)(\d+)", "\g<1>_\g<2>", regex=True)  # noqa: W605
        .str.replace("strasse_", "str_", regex=False)
        .str.replace("_str_", "str_", regex=False)
        # .str.replace("_weg_", "weg_", regex=False)
        # .str.replace("_platz_", "platz_", regex=False) # schlecht bei: "Platz der Einheit"
        .str.replace("_+", "_", regex=True)
        .str.strip("_")
    )
    se.name = name
    return se
