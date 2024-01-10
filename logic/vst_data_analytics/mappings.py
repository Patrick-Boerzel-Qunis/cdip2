import numpy as np

COLUMN_DEFINITIONS = {
    "Bisnode": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": "Int64",
        },
        "STATUS": {
            "name": "Status",
            "type": "string[pyarrow]",
        },
        "FIRMENNAME": {
            "name": "Firmenname",
            "type": "string[pyarrow]",
        },
        "FIRMENTYP_LANGTEXT": {
            "name": "Firmentype",
            "type": "string[pyarrow]",
        },
        "HANDELSNAME": {
            "name": "Handelsname",
            "type": "string[pyarrow]",
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
        },
        "PLZ_STRASSE": {
            "name": "PLZ",
            "type": "string[pyarrow]",
        },
        "ORT_STRASSE": {
            "name": "Ort",
            "type": "string[pyarrow]",
        },
        "STRASSE": {
            "name": "Strasse",
            "type": "string[pyarrow]",
        },
        "HAUSNUMMER": {
            "name": "Hausnummer",
            "type": "string[pyarrow]",
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": "string[pyarrow]",
        },
        "GKZ": {
            "name": "GKZ",
            "type": "Int64",
        },
        "VORWAHL_TELEFON": {
            "name": "Vorwahl_Telefon",
            "type": "string[pyarrow]",
        },
        "TELEFON": {
            "name": "Telefon",
            "type": "string[pyarrow]",
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": "Float64",
        },
        "UMSATZ_JAHR": {
            "name": "Umsatz_Jahr",
            "type": "Float64",
        },
        "SEGMENT_DICC": {
            "name": "Segment",
            "type": "Float64",
        },
        "MARKETABLE": {
            "name": "Marketable",
            "type": "string[pyarrow]",
        },
        "HAUPTBRANCHE_08": {
            "name": "Hauptbranche",
            "type": "string[pyarrow]",
        },
        "HAUPTBRANCHENTEXT_08": {
            "name": "Hauptbranchentext",
            "type": "string[pyarrow]",
        },
        "NEBENBRANCHE_08": {
            "name": "Nebenbranche",
            "type": "string[pyarrow]",
        },
        "ANZAHL_NIEDERLASSUNGEN": {
            "name": "Anzahl_Niederlassungen",
            "type": "string[pyarrow]",
        },
        "ANZAHL_KONZERNMITGLIEDER": {
            "name": "Anzahl_Konzernmitglieder",
            "type": "Int64",
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": "Float64",
        },
        "DIREKTE_MUTTER_NUMMER": {
            "name": "Direkte_Mutter_Nummer",
            "type": "Float64",
        },
        "DIREKTE_MUTTER_LAND": {
            "name": "Direkte_Mutter_Land",
            "type": "string[pyarrow]",
        },
        "HoechsteMutterNummer_0": {
            "name": "HNR",
            "type": "Float64",
        },
        "RECHTSFORM_TEXT": {
            "name": "Rechtsform",
            "type": "string[pyarrow]",
        },
        "EHEMALIGER_FIRMENNAME": {
            "name": "Ehemaliger_Firmenname",
            "type": "string[pyarrow]",
        },
        "HANDELSREGISTER": {
            "name": "Register",
            "type": "string[pyarrow]",
        },
    },
    "BisnodePrimus": {
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
        },
        "GESCHLECHT_TEXT_1": {
            "name": "Geschlecht_Text_1",
            "type": "string[pyarrow]",
        },
        "GESCHLECHT_TEXT_2": {
            "name": "Geschlecht_Text_2",
            "type": "string[pyarrow]",
        },
        "GESCHLECHT_TEXT_3": {
            "name": "Geschlecht_Text_3",
            "type": "string[pyarrow]",
        },
        "TITEL_1": {
            "name": "Titel_1",
            "type": "string[pyarrow]",
        },
        "TITEL_2": {
            "name": "Titel_2",
            "type": "string[pyarrow]",
        },
        "TITEL_3": {
            "name": "Titel_3",
            "type": "string[pyarrow]",
        },
        "NAME_1": {
            "name": "Name_1",
            "type": "string[pyarrow]",
        },
        "NAME_2": {
            "name": "Name_2",
            "type": "string[pyarrow]",
        },
        "NAME_3": {
            "name": "Name_3",
            "type": "string[pyarrow]",
        },
        "VORNAME_1": {
            "name": "Vorname_1",
            "type": "string[pyarrow]",
        },
        "VORNAME_2": {
            "name": "Vorname_2",
            "type": "string[pyarrow]",
        },
        "VORNAME_3": {
            "name": "Vorname_3",
            "type": "string[pyarrow]",
        },
        "POSITION_TEXT_1": {
            "name": "Position_Text_1",
            "type": "string[pyarrow]",
        },
        "POSITION_TEXT_2": {
            "name": "Position_Text_2",
            "type": "string[pyarrow]",
        },
        "POSITION_TEXT_3": {
            "name": "Position_Text_3",
            "type": "string[pyarrow]",
        },
        "INTERNET_ADRESSE": {
            "name": "Website",
            "type": "string[pyarrow]",
        },
        "EMAIL": {
            "name": "Email",
            "type": "string[pyarrow]",
        },
        "FIRMENZENTRALE_AUSLAND": {
            "name": "Firmenzentrale_Ausland",
            "type": "string[pyarrow]",
        },
    },
    "BeDirect": {
        "BE_ID": {
            "name": "BED_ID",
            "type": "Int64",
        },
        "BE_FIRMENNAME_GESAMT": {
            "name": "Firmenname",
            "type": "string[pyarrow]",
        },
        "BE_PLZ": {
            "name": "PLZ",
            "type": "string[pyarrow]",
        },
        "BE_ORT": {
            "name": "Ort",
            "type": "string[pyarrow]",
        },
        "BE_STRASSE": {
            "name": "Strasse",
            "type": "string[pyarrow]",
        },
        "BE_HAUSNUMMER": {
            "name": "Hausnummer",
            "type": "string[pyarrow]",
        },
        "BE_VORWAHL": {
            "name": "Vorwahl_Telefon",
            "type": "string[pyarrow]",
        },
        "BE_RUFNUMMER": {
            "name": "Telefon",
            "type": "string[pyarrow]",
        },
        "BE_E_MAIL": {
            "name": "Email",
            "type": "string[pyarrow]",
        },
        "BE_HOMEPAGE": {
            "name": "Website",
            "type": "string[pyarrow]",
        },
        "BE_PRIMAERBRANCHE": {
            "name": "Hauptbranche",
            "type": "string[pyarrow]",
        },
        "BE_BRANCHE2": {
            "name": "Nebenbranche_1",
            "type": "string[pyarrow]",
        },
        "BE_BRANCHE3": {
            "name": "Nebenbranche_2",
            "type": "string[pyarrow]",
        },
        "BE_BRANCHE4": {
            "name": "Nebenbranche_3",
            "type": "string[pyarrow]",
        },
        "BE_BRANCHE5": {
            "name": "Nebenbranche_4",
            "type": "string[pyarrow]",
        },
        "BE_MITARBEITERSTAFFEL": {
            "name": "Beschaeftigte_Code",
            "type": "string[pyarrow]",
        },
        "BE_ANZAHL_NL_FILIALEN": {
            "name": "Anzahl_Niederlassungen",
            "type": "string[pyarrow]",
        },
        "HR_TYP": {
            "name": "Register_Type",
            "type": "string[pyarrow]",
        },
        "HR_NUMMER": {
            "name": "Register_Nummer",
            "type": "string[pyarrow]",
        },
        "BE_UMSATZSTAFFEL": {
            "name": "Umsatz_Code",
            "type": "string[pyarrow]",
        },
        "TELEFON_SELECT": {
            "name": "Tel_Select",
            "type": "string[pyarrow]",
        },
        "FLAG_QUALITAET_ORG": {
            "name": "Flag_Quality",
            "type": "string[pyarrow]",
        },
        "BE_ANREDE": {
            "name": "Geschlecht_Text",
            "type": "string[pyarrow]",
        },
        "BE_TITEL": {
            "name": "Titel",
            "type": "string[pyarrow]",
        },
        "BE_VORNAME": {
            "name": "Vorname",
            "type": "string[pyarrow]",
        },
        "BE_NACHNAME": {
            "name": "Name",
            "type": "string[pyarrow]",
        },
        "BE_PREFIX": {
            "name": "Prefix_Name",
            "type": "string[pyarrow]",
        },
        "BIPID_DIREKTE_MUTTER": {
            "name": "Direkte_Mutter_Nummer",
            "type": "Int64",
        },
        "BIPID_HOECHSTE_MUTTER": {
            "name": "HNR",
            "type": "Int64",
        },
        "BE_RECHTSFORM_ID": {
            "name": "Rechtsform",
            "type": "string[pyarrow]",
        },
    },
    "MapPlzBundesland": {
        "PLZ": {
            "name": "PLZ",
            "type": "string[pyarrow]",
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": "string[pyarrow]",
        },
    },
    "MapBedBranche": {
        "BRANCHE": {
            "name": "Hauptbranche",
            "type": "string[pyarrow]",
        },
        "BRANCHENTEXT": {
            "name": "Hauptbranchentext",
            "type": "string[pyarrow]",
        },
    },
    "BisnodeForBeD": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": "Int64",
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": "Float64",
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": "Float64",
        },
    },
    "Industriescore": {
        "WZ8_H5_CODE": {
            "name": "Hauptbranche",
            "type": "string[pyarrow]",
        },
        "INDUSTRY_SCORE": {
            "name": "Industry_Score",
            "type": "Int64",
        },
    },
}

MAP_TITLE = {
    "Ingenieur": np.nan,
    "Dr. med.": "Dr.",
    "nan": np.nan,
    "Baron": np.nan,
    "Rechtsanwalt": np.nan,
    "Professor": "Prof.",
    "Baroness": np.nan,
    "Gräfin": np.nan,
    "Graf": np.nan,
    "Freifrau": np.nan,
    "Freiherr": np.nan,
    "Ritter": np.nan,
    "Prinzessin": np.nan,
    "Edler": np.nan,
    "Dr. Ing.": "Dr.",
    "Prof. Dr. med.": "Prof. Dr.",
    "Prof. Dr. Dr.": "Prof. Dr.",
    "Dr. Dr.": "Dr.",
    "Dr. mult.": "Dr.",
    "Dr. med": "Dr.",
    "Dr. h.c.": "Dr.",
    "Prof. Dr. Dr. h. c.": "Prof. Dr.",
    "Dr. Ingenieur": "Dr.",
    "Botschafter": np.nan,
    "Dr. Graf": "Dr.",
    "Prof. Dr. Ingenieur": "Prof. Dr.",
    "Exzellenz": np.nan,
    "Dr. Ing. Ingenieur": "Dr.",
    "Dr. Baron": "Dr.",
    "Notar": np.nan,
    "Professor Dr.": "Prof. Dr.",
    "Professor Dr. Ing.": "Prof. Dr.",
    "Hauptmann": np.nan,
}

MAP_GENDER = {
    "männlich": "Herr",
    "weiblich": "Frau",
    "Freiherr": "Herr",
    "Freifrau": "Frau",
}

MAP_REV_MEDIAN = {
    "00": np.nan,
    "01": "0.130",
    "02": "0.170",
    "03": "0.270",
    "04": "0.630",
    "05": "1.935",
    "06": "5.720",
    "07": "30.000",
    "08": "78.316",
    "09": "670.832",
}

MAP_EMPL_MEDIAN = {
    "00": np.nan,
    "01": "2",
    "02": "6",
    "03": "12",
    "04": "26",
    "05": "61",
    "06": "121",
    "07": "251",
    "08": "590",
    "09": "1146",
    "10": "2320",
}

RECHTSREFORM_MAPPING = {
    "3": "AG",
    "4": "keine RF, Einzelfirma, Behörde, Institution etc.",
    "5": "und Partner",
    "7": "AG & Co.",
    "8": "AG & Co. KG",
    "9": "AG & Co. oHG",
    "10": "AÖR",
    "11": "eG",
    "12": "EWIV",
    "13": "GbR",
    "14": "GmbH",
    "15": "GmbH & Co.",
    "16": "GmbH & Co. KG",
    "17": "GmbH & Co. oHG",
    "18": "KG",
    "19": "KGaA",
    "20": "mbH",
    "21": "mbH & Co.",
    "22": "mbH & Co. KG",
    "23": "mbH & Co. oHG",
    "24": "oHG",
    "26": "Partnerschaft",
    "27": "VVaG",
    "28": "e. V.",
    "29": "e. Kfm.",
    "30": "e. Kfr.",
    "31": "e.K.",
    "32": "UG (haftungsbeschränkt)",
    "33": "UG (haftungsbeschränkt) &  Co. KG",
    "34": "SE",
    "35": "SE & Co. KG",
    "36": "SE & Co. KGaA",
    "37": "AG & Co. KGaA",
    "45": "aG",
    "46": "gGmbH",
    "47": "VGaG",
    "48": "Arge",
    "49": "Limited",
    "50": "Limited & Co. KG",
    "51": "AG & Co. KGaA",
}
