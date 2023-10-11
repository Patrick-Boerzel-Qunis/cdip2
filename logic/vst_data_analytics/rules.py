import requests

import numpy as np
import pandas as pd


def AUR02_DnB(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(
        Titel_1=lambda x: x.Titel_1.map(mapping),
        Titel_2=lambda x: x.Titel_2.map(mapping),
        Titel_3=lambda x: x.Titel_3.map(mapping),
    )


def AUR02_BeD(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(Titel=lambda x: x.Titel.map(mapping))


def AUR03_DnB(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(
        Geschlecht_Text_1=lambda x: x.Geschlecht_Text_1.map(mapping),
        Geschlecht_Text_2=lambda x: x.Geschlecht_Text_2.map(mapping),
        Geschlecht_Text_3=lambda x: x.Geschlecht_Text_3.map(mapping),
    )


def AUR03_BeD(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(Geschlecht_Text=lambda x: x.Geschlecht_Text.replace(mapping))


def AUR08(df: pd.DataFrame, mapping: dict[str, float]) -> pd.DataFrame:
    return df.assign(Umsatz=lambda x: x.Umsatz_Code.replace(mapping).astype(np.float32))


def AUR09(df: pd.DataFrame, mapping: dict[str, int]) -> pd.DataFrame:
    return df.assign(
        Beschaeftigte=lambda x: x.Beschaeftigte_Code.replace(mapping).astype(np.float32)
    )


def AUR11(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Handelregister=lambda x: x.Register_Type + x.Register_Nummer)


def AUR12(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Name=lambda x: np.where(
            x["Prefix_Name"].isna(), x["Name"], x["Prefix_Name"] + " " + x["Name"]
        )
    )


def AUR16(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Nebenbranche=lambda y: (
            y["Nebenbranche_1"].fillna("_____").str.rjust(5, "0")
            + ";"
            + y["Nebenbranche_2"].fillna("_____").str.rjust(5, "0")
            + ";"
            + y["Nebenbranche_3"].fillna("_____").str.rjust(5, "0")
            + ";"
            + y["Nebenbranche_4"].fillna("_____").str.rjust(5, "0")
        )
        .str.replace(";_____", "")
        .replace("_____", "")
        .replace("", np.NaN)
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


def AUR04(df: pd.DataFrame) -> pd.DataFrame:
    df["Telefon_complete"] = (
        "+49 " + df["Vorwahl_Telefon"].str[1:] + " " + df["Telefon"]
    )
    return df


def AUR06(df: pd.DataFrame) -> pd.DataFrame:
    re_tele = "((0{1})([1]{1})([3567]{1})([0-9]{1,2}))|(0700)|(0800)|(0900)"
    df["Telefon_Type"] = np.where(
        (~df["Vorwahl_Telefon"].str.match(re_tele, na=True))
        & (df["Vorwahl_Telefon"].notna()),
        "Fixed network",
        np.where(
            (df["Vorwahl_Telefon"].notna()),
            "Mobile / Premium",
            df["Vorwahl_Telefon"],
        ),
    )
    return df


def AUR07(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Bundesland=lambda x: x.Bundesland.str.title())


def AUR10(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Umsatz=lambda x: x.Umsatz.astype(np.float32))


def AUR13(df: pd.DataFrame) -> pd.DataFrame:
    if "Marketable" in df.columns:
        df = df.assign(Marketable=lambda x: x.Marketable.fillna("N"))
    if "Firmenzentrale_Ausland" in df.columns:
        df = df.assign(
            Firmenzentrale_Ausland=lambda x: x.Firmenzentrale_Ausland.fillna("N")
        )
    if "Tel_Select" in df.columns:
        df.assign(Tel_Select=lambda x: x.Tel_Select.replace("J", "Y"))
    return df


def AUR14(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Hauptbranche=lambda x: x.Hauptbranche.str.ljust(5, "0"))


#   - AUR17: #Core
#       source_name: "datenschutz"
#       data_path: "anreicherung_tables"
#       file_name: "datenschutz.csv"
#       description: "Remove wrong phone/email entries"


def AUR18(df: pd.DataFrame) -> pd.DataFrame:
    df["Strasse"].replace(["o.A", "o. A", "o.A.", "o. A."], np.NaN, inplace=True)
    return df


def AUR19(df: pd.DataFrame) -> pd.DataFrame:
    df["Telefon_complete"].replace(
        to_replace=r"^[0\+]{1,2}49[0 ]*$", value=np.NaN, regex=True, inplace=True
    )
    return df


def AUR20(df: pd.DataFrame) -> pd.DataFrame:
    df["Telefon_complete"].replace(
        to_replace=r"^([0\+]{1,2}49)[0 ]*([1-9][ 0-9]*)$",
        value=r"\1 \2",
        regex=True,
        inplace=True,
    )
    return df


def AUR21(df: pd.DataFrame) -> pd.DataFrame:
    df["Hausnummer"] = df["Hausnummer"].str.replace(" ", "").str.casefold()
    return df


def AUR108(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Name=lambda x: np.where(
            pd.isna(x.Name_3), np.where(pd.isna(x.Name_1), x.Name_2, x.Name_1), x.Name_3
        ),
        Vorname=lambda x: np.where(
            pd.isna(x.Vorname_3),
            np.where(pd.isna(x.Vorname_1), x.Vorname_2, x.Vorname_1),
            x.Vorname_3,
        ),
        Geschlecht_Text=lambda x: np.where(
            pd.isna(x.Geschlecht_Text_3),
            np.where(
                pd.isna(x.Geschlecht_Text_1), x.Geschlecht_Text_2, x.Geschlecht_Text_1
            ),
            x.Geschlecht_Text_3,
        ),
        Position_Text=lambda x: np.where(
            pd.isna(x.DNB_Position_Text_3),
            np.where(
                pd.isna(x.DNB_Position_Text_1),
                x.DNB_Position_Text_2,
                x.DNB_Position_Text_1,
            ),
            x.DNB_Position_Text_3,
        ),
        Titel=lambda x: np.where(
            pd.isna(x.Titel_3),
            np.where(pd.isna(x.Titel_1), x.Titel_2, x.Titel_1),
            x.Titel_3,
        ),
    )


def AUR109(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Status=lambda x: np.where(x.Status == "inaktiv", False, True),
    )


#   - AUR111: "Remove Handelsname where Handelsname equals to Ort" #Cleanse


def AAR10(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    return df.assign(Rechtsform=lambda x: x.Rechtsform.replace(mapping))


def AAR050(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Anzahl_Niederlassungen=lambda x: x.Anzahl_Niederlassungen.replace(
            {"None": np.nan}
        ).astype(np.float32),
    )


def get_umsatz_score(df_bisnode: pd.DataFrame) -> pd.DataFrame:
    df = (
        df_bisnode.replace("None", np.NaN)
        .assign(Umsatz=lambda x: x.Umsatz.astype(np.float32))
        .assign(
            Umsatz_Score=lambda x: pd.cut(
                x.Umsatz,
                bins=[-np.inf, 10, 50, 250, 500, np.inf],
                labels=[1, 2, 3, 4, 5],
            ).astype(np.float32),
            Umsatz_Code=lambda x: pd.cut(
                x.Umsatz,
                bins=[-np.inf, 0.1, 0.25, 0.5, 2.5, 5, 25, 50, 500, np.inf],
                labels=["01", "02", "03", "04", "05", "06", "07", "08", "09"],
            ),
        )
    )
    g = df.groupby(["Umsatz_Code"])
    # Ziel: In jeder Staffel den gemittelten Durchschnitt von den entsprechenden Bisnode-Werten berechnen
    return pd.DataFrame(
        {"Umsatz": g.Umsatz.mean(), "Umsatz_Score": g.Umsatz_Score.mean()}
    )


def get_beschaeftigte_score(df_bisnode: pd.DataFrame) -> pd.DataFrame:
    df = (
        df_bisnode.replace("None", np.NaN)
        .assign(Beschaeftigte=lambda x: x.Beschaeftigte.astype(np.float32))
        .assign(
            Beschaeftigte_Score=lambda x: pd.cut(
                x.Beschaeftigte,
                bins=[-np.inf, 10, 50, 250, 999, np.inf],
                labels=[1, 2, 3, 4, 5],
            ).astype(np.float32),
            Beschaeftigte_Code=lambda x: pd.cut(
                x.Beschaeftigte,
                bins=[0, 4, 9, 19, 49, 99, 199, 499, 999, 1999, np.inf],
                labels=["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"],
            ),
        )
    )
    g = df.groupby(["Beschaeftigte_Code"])
    # Ziel: In jeder Staffel den gemittelten Durchschnitt von den entsprechenden Bisnode-Werten berechnen
    return pd.DataFrame(
        {
            "Beschaeftigte": g.Beschaeftigte.mean(),
            "Beschaeftigte_Score": g.Beschaeftigte_Score.mean(),
        }
    )


def AAR051(df_left: pd.DataFrame, df_right: pd.DataFrame) -> pd.DataFrame:
    _index_name: str = df_left.index.name
    _result: pd.DataFrame = None
    if _index_name is None:
        _result = df_left.merge(
            get_umsatz_score(df_right), how="left", on="Umsatz_Code"
        ).merge(get_beschaeftigte_score(df_right), how="left", on="Beschaeftigte_Code")
    else:
        _result = (
            df_left.reset_index()
            .merge(get_umsatz_score(df_right), how="left", on="Umsatz_Code")
            .merge(
                get_beschaeftigte_score(df_right), how="left", on="Beschaeftigte_Code"
            )
            .set_index(_index_name)
        )

    return _result


def AAR053(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(Umsatz_pro_Mitarbeiter=lambda x: x.Umsatz / x.Beschaeftigte)


def AAR054(df: pd.DataFrame) -> pd.DataFrame:
    df["Umsatz_pro_Mitarbeiter_Score"] = pd.cut(
        df.Umsatz_pro_Mitarbeiter,
        bins=[0, 0.1, 0.2, 0.3, 0.5, np.inf],
        labels=[1, 2, 3, 4, 5],
    ).astype(np.float32)

    return df


def AAR055(df: pd.DataFrame) -> pd.DataFrame:
    df["Niederlassungs_Score"] = pd.cut(
        df.Anzahl_Niederlassungen,
        bins=[0, 1, 2, 5, 10, np.inf],
        labels=[1, 2, 3, 4, 5],
    ).astype(np.float32)

    return df


def AAR056(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        Anzahl_Konzernmitglieder=lambda x: x.groupby(["HNR"])["HNR"]
        .transform("count")
        .astype(np.float32)
    )


def AAR057(df: pd.DataFrame) -> pd.DataFrame:
    df["Konzernmitglieder_Score"] = pd.cut(
        df.Anzahl_Konzernmitglieder,
        bins=[0, 4, 50, 100, 200, np.inf],
        labels=[1, 2, 3, 4, 5],
    ).astype(np.float32)

    return df


def AAR058(df: pd.DataFrame) -> pd.DataFrame:
    for col in [
        "Umsatz_Score",
        "Beschaeftigte_Score",
        "Umsatz_pro_Mitarbeiter_Score",
        "Niederlassungs_Score",
        "Konzernmitglieder_Score",
        "Industry_Score",
    ]:
        df[col].fillna(1, inplace=True)

    df["Gesamt_Score"] = (
        df["Umsatz_Score"] * 0.2
        + df["Beschaeftigte_Score"] * 0.2
        + df["Umsatz_pro_Mitarbeiter_Score"] * 0.1
        + df["Niederlassungs_Score"] * 0.1
        + df["Konzernmitglieder_Score"] * 0.2
        + df["Industry_Score"] * 0.2
    ).astype(np.float32)

    return df


def _add_segment_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Segment"] = pd.cut(
        df.Gesamt_Score,
        bins=[0, 2, 3.5, 5],
        labels=[3, 2, 1],
    ).astype(np.float32)

    return df


def _rule_segment_anzahl_konzernmitglieder(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df["Segment"] == 3) & (df["Anzahl_Konzernmitglieder"] > 1),
        "Segment",
    ] = 2
    return df


def _rule_segment_anzahl_niederlassungen(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df["Segment"] == 3) & (df["Anzahl_Niederlassungen"] > 1),
        "Segment",
    ] = 2
    return df


def _rule_firmenname(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        df["Firmenname"].str.contains(
            "|".join(
                [
                    "Volksbank",
                    "Raiffeisen",
                    "Landkreis",
                    "Landratsamt",
                    "Bezirk",
                    "Industrie- und Handelskammer",
                    "VOLKSBANK",
                    "Volksbanken",
                    "Sparkasse",
                    "Sparkassen",
                    "sparkasse",
                    "Ministerium",
                    "ministerium",
                ]
            )
        ),
        "Segment",
    ] = 1
    return df


def _rule_umsatz_code(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["Umsatz_Code"].astype(np.float32) >= 9, "Segment"] = 1
    return df


def _rule_umsatz_beschaeftigte_code(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df["Umsatz_Code"].astype(np.float32) >= 3)
        & (df["Beschaeftigte_Code"].astype(np.float32) >= 9),
        "Segment",
    ] = 1
    return df


def _rule_umsatz_branche(df: pd.DataFrame) -> pd.DataFrame:
    # unklar, wie das umgesetzt werden kann
    # da Umsatzstaffeln nicht 10 Mio als Grenze haben
    # und es gibt nur folgende Branchen:
    # 6209: Erbringung von sonstigen Dienstleistungen der Informationstechnologie
    # 6203: Betrieb von Datenverarbeitungseinrichtungen für Dritte
    df.loc[
        (df["Umsatz_Code"].astype(np.float32) >= 6)
        & ((df["Hauptbranche"] == 6209) | (df["Hauptbranche"] == 6203)),
        "Segment",
    ] = 1
    return df


def _rule_anzahl_niederlassungen(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["Anzahl_Niederlassungen"] > 50, "Segment"] = 1
    return df


def _rule_rechtsform(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[
        (df["Rechtsform"] == "KGaA")
        | (df["Rechtsform"] == "SE")
        | (df["Rechtsform"] == "AG")
        | (df["Rechtsform"] == "AG & Co. oHG")
        | (df["Rechtsform"] == "AG & Co. KG"),
        "Segment",
    ] = 1
    return df


def _rule_anzahl_konzernmitglieder(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["Anzahl_Konzernmitglieder"] > 50, "Segment"] = 1
    return df


def _rule_konzernsegment(df: pd.DataFrame) -> pd.DataFrame:
    # Konzernsegment:
    # numerisch kleinstes Segment unter allen mit selber höchster Mutter
    df["Konzernsegment"] = df.groupby(["HNR"])["Segment"].transform("min")
    df["Konzernsegment"].fillna(df["Segment"], inplace=True)
    # Beschaeftigte_Konzern:
    # Summe der Mitarbeiter unter einer höchsten Nummer
    df["Beschaeftigte_Konzern"] = df.groupby(["HNR"])["Beschaeftigte"].transform("sum")

    df.loc[(df["Segment"] >= 2) & (df["Konzernsegment"] == 1), "Segment"] = 1

    df.loc[
        (df["Segment"] == 2)
        & (df["Konzernsegment"] == 2)
        & (df["Beschaeftigte_Konzern"] <= 50),
        "Segment",
    ] = 3

    df.loc[
        (df["Segment"] == 3)
        & ((df["Konzernsegment"] == 2) | (df["Konzernsegment"] == 3))
        & (df["Beschaeftigte_Konzern"] > 50),
        "Segment",
    ] = 2

    df.loc[
        (df["Segment"] == 2)
        & (df["Konzernsegment"] == 3)
        & (df["Beschaeftigte_Konzern"] <= 50),
        "Segment",
    ] = 3

    return df


def AAR059(df_bed: pd.DataFrame) -> pd.DataFrame:
    """
    Update segment based on miscellaneous rules.

    Parameters
    ----------
    df : pd.DataFrame
        BeDirect data with segment information which needs to be updated.

    Note : The function in pipeline is as per priority. The first function has lowest priority and

    Returns
    -------
    Finalized segment data.

    """
    return (
        df_bed.pipe(_add_segment_data)
        .pipe(_rule_segment_anzahl_konzernmitglieder)
        .pipe(_rule_segment_anzahl_niederlassungen)
        .pipe(_rule_firmenname)
        .pipe(_rule_umsatz_code)
        .pipe(_rule_umsatz_beschaeftigte_code)
        .pipe(_rule_umsatz_branche)
        .pipe(_rule_anzahl_niederlassungen)
        .pipe(_rule_rechtsform)
        .pipe(_rule_anzahl_konzernmitglieder)
        .pipe(_rule_konzernsegment)
    )


def AUR111(df):
    df.loc[df.Handelsname == df.Ort, "Handelsname"] = np.nan
    return df


def AddressMaster(
    df: pd.DataFrame,
    url: str,
    headers: dict[str, str] = None,
    batch_size=10_000,
    index_key="GP_RAW_ID",
) -> pd.DataFrame:
    _headers = headers or dict()
    data_size = df.shape[0]
    tasks = []
    _from = 0
    while _from < data_size:
        _to = _from + batch_size if _from + batch_size < data_size else data_size
        tasks.append(df.iloc[_from:_to])
        _from = _to

    responses = []
    for task in tasks:
        try:
            _json_payload = (
                task.assign(address=lambda x: x.fillna("").to_dict("records"))[
                    [index_key, "address"]
                ]
                .rename(columns={index_key: "requestId"})
                .to_json(orient="records", force_ascii=True)
            )
            _response_api = requests.post(url, data=_json_payload, headers=_headers)
            if not _response_api.ok:
                print(
                    f"Error: VT AddressMaster request failed: {_response_api.status_code}"
                )
                print(_response_api.content.decode("UTF-8"))
                continue
            df_res: pd.DataFrame = pd.read_json(
                path_or_buf=_response_api.content.decode()
            )
            am: pd.DataFrame = pd.concat(
                [
                    df_res.drop(["validationResult"], axis=1),
                    pd.json_normalize(df_res["validationResult"]),
                ],
                axis=1,
            ).rename(columns={"requestId": index_key})
            am.dropna(thresh=am.shape[1] - 3, inplace=True)
            # am[index_key] = am[index_key].astype(int)
            am["exactAddress.addressId"] = am["exactAddress.addressId"].astype(int)
            # df_res = task.assign(GP_RAW_ID=lambda x: x[index_key].astype(np.float32)).join(other=am.set_index(index_key), on=index_key)
            # am.set_index(index_key, inplace=True)
            print(f"TASK cols: {task.columns}")
            print(f"am columns: {am.columns}")
            print(f"JOIN ON {index_key}")
            df_res = task.merge(am, on=index_key, how="left")
            df_res = df_res.rename(
                columns={
                    "exactAddress.streetNr": "VT_Hausnummer",
                    "exactAddress.city": "VT_Ort",
                    "exactAddress.postCode": "VT_PLZ",
                    "exactAddress.street": "VT_Strasse",
                    "exactAddress.source": "VT_source",
                    "exactAddress.state": "VT_Bundesland",
                    "exactAddress.district": "VT_Ortsteil",
                    "exactAddress.streetNrSuffix": "VT_Hausnummernzusatz",
                    "exactAddress.addressId": "VT_addressId",
                    "exactAddress.klsId": "VT_DTAG_addressId",
                    "exactAddress.hereId": "VT_Nokia_Here_addressId",
                    "exactAddress.addition.source": "VT_Addition_source",
                    "exactAddress.addition.xEtrs89": "VT_Langengrad_etrs89",
                    "exactAddress.addition.yEtrs89": "VT_Breitengrad_etrs89",
                    "exactAddress.addition.areaCode": "VT_Ortsnetzkennzahl",
                    "exactAddress.addition.agsn": "VT_agsn",
                }
            ).drop(columns=["possibleAddresses"])
            if "exactAddress" in df_res.columns:
                df_res.drop(columns=["exactAddress"], inplace=True)
            # the possibleAddresses column contains alternative match, JSON encoded, with the same format as exact match address
            # it needs to be parsed, or normalized, as pandas does not support JSON type in 'object' type column to be stored in DB
            # (to_sql method fails with exception). Need to specify colum to be of JSON type in order to_sql would work
            df_res = (
                df_res.assign(
                    Exact_Bundesland=lambda x: np.where(
                        x["state"] != x["VT_Bundesland"], x["VT_Bundesland"], np.NaN
                    ),
                    Exact_Hausnummer=lambda x: np.where(
                        x["streetNr"] != x["VT_Hausnummer"], x["VT_Hausnummer"], np.NaN
                    ),
                    Exact_Strasse=lambda x: np.where(
                        x["street"] != x["VT_Strasse"], x["VT_Strasse"], np.NaN
                    ),
                    Exact_PLZ=lambda x: np.where(
                        x["postCode"] != x["VT_PLZ"], x["VT_PLZ"], np.NaN
                    ),
                    Exact_Ort=lambda x: np.where(
                        x["city"] != x["VT_Ort"], x["VT_Ort"], np.NaN
                    ),
                    Bundesland=lambda x: np.where(
                        x["Exact_Bundesland"].isnull(),
                        x["state"],
                        x["Exact_Bundesland"],
                    ),
                    Hausnummer=lambda x: np.where(
                        x["Exact_Hausnummer"].isnull(),
                        x["streetNr"],
                        x["Exact_Hausnummer"],
                    ),
                    PLZ=lambda x: np.where(
                        x["Exact_PLZ"].isnull(), x["postCode"], x["Exact_PLZ"]
                    ),
                    Ort=lambda x: np.where(
                        x["Exact_Ort"].isnull(), x["city"], x["Exact_Ort"]
                    ),
                    Strasse=lambda x: np.where(
                        x["Exact_Strasse"].isnull(), x["street"], x["Exact_Strasse"]
                    ),
                )
                .drop(columns=["state", "streetNr", "street", "postCode", "city"])
                .drop(
                    columns=[
                        "VT_PLZ",
                        "VT_source",
                        "VT_Bundesland",
                        "VT_Ort",
                        "VT_Ortsteil",
                        "VT_Strasse",
                        "VT_Hausnummer",
                        "VT_Hausnummernzusatz",
                        "VT_DTAG_addressId",
                        "VT_Nokia_Here_addressId",
                        "VT_Addition_source",
                        "VT_Ortsnetzkennzahl",
                        "VT_agsn",
                        "VT_Langengrad_etrs89",
                        "VT_Breitengrad_etrs89",
                        "Exact_Bundesland",
                        "Exact_Hausnummer",
                        "Exact_Strasse",
                        "Exact_PLZ",
                        "Exact_Ort",
                    ]
                )
            )
            responses.append(df_res)
            print("Info: Batch processed.")
        except Exception as err:
            print(f"Error: VT Address master loop threw exception: {err}")
    print("Info: VT Address Master: completed.")
    return pd.concat(responses)
