import numpy as np
from pyspark.pandas import DataFrame


def AUR02_BeD(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    return df.assign(Titel=lambda x: x.Titel.map(mapping))


def AUR03_BeD(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    return df.assign(Geschlecht_Text=lambda x: x.Geschlecht_Text.replace(mapping))


def AUR08(df: DataFrame, mapping: dict[str, float]) -> DataFrame:
    return df.assign(Umsatz=lambda x: x.Umsatz_Code.replace(mapping).astype(np.float32))


def AUR09(df: DataFrame, mapping: dict[str, int]) -> DataFrame:
   return df.assign(Beschaeftigte=lambda x: x.Beschaeftigte_Code.replace(mapping).astype(np.float32))


def AUR11(df: DataFrame) -> DataFrame:
    return df.assign(Handelregister=lambda x: x.Register_Type + x.Register_Nummer)


def AUR12(df: DataFrame) -> DataFrame:
    return df.assign(Name=lambda x: np.where(
            x["Prefix_Name"].isna(),
            x["Name"],
            x["Prefix_Name"] + " " + x["Name"]
        )
    )


def AUR16(df: DataFrame) -> DataFrame:
    return df.assign(
        Nebenbranche=lambda y:
        (y["Nebenbranche_1"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_2"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_3"].fillna("_____").str.rjust(5, "0") + ";" +
            y["Nebenbranche_4"].fillna("_____").str.rjust(5, "0")).str.replace(";_____", "").replace("_____","").replace("", np.NaN)
    )
