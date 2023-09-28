import numpy as np
import pandas as pd

def AUR02_BeD(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(Titel=lambda x: x.Titel.map(mapping))


def AUR03_BeD(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(Geschlecht_Text=lambda x: x.Geschlecht_Text.replace(mapping))


def AUR08(df: pd.DataFrame, mapping: dict[str, float]) -> pd.DataFrame:
    return df.assign(Umsatz=lambda x: x.Umsatz_Code.replace(mapping).astype(np.float32))


def AUR09(df: pd.DataFrame, mapping: dict[str, int]) -> pd.DataFrame:
   return df.assign(Beschaeftigte=lambda x: x.Beschaeftigte_Code.replace(mapping).astype(np.float32))


def AUR11(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Handelregister=lambda x: x.Register_Type + x.Register_Nummer)


def AUR12(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Name=lambda x: np.where(
            x["Prefix_Name"].isna(),
            x["Name"],
            x["Prefix_Name"] + " " + x["Name"]
        )
    )


def AUR16(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Nebenbranche=lambda y:
        (y["Nebenbranche_1"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_2"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_3"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_4"].fillna("_____").str.rjust(5, "0")).str.replace(";_____", "").replace("_____","").replace("", np.NaN)
    )


def AUR104(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(
        columns=[
            "Register_Type",
            "Register_Nummer",
            "Nebenbranche_1",
            "Nebenbranche_2",
            "Nebenbranche_3",
            "Nebenbranche_4",
        ]
    )


def AUR110(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        HNR=lambda x: np.where(x.HNR.isna(), x.Direkte_Mutter_Nummer, x.HNR),
    )

