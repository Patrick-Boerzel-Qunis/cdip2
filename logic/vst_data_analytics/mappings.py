import numpy as np

COLUMN_DEFINITIONS = {
    "Bisnode": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": "Int64",
        },
        "STATUS": {
            "name": "Status",
            "type": "object",
        },
        "FIRMENNAME": {
            "name": "Firmenname",
            "type": "object",
        },
        "FIRMENTYP_LANGTEXT": {
            "name": "Firmentype",
            "type": "object",
        },
        "HANDELSNAME": {
            "name": "Handelsname",
            "type": "object",
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
        },
        "PLZ_STRASSE": {
            "name": "PLZ",
            "type": "object",
        },
        "ORT_STRASSE": {
            "name": "Ort",
            "type": "object",
        },
        "STRASSE": {
            "name": "Strasse",
            "type": "object",
        },
        "HAUSNUMMER": {
            "name": "Hausnummer",
            "type": "object",
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": "object",
        },
        "GKZ": {
            "name": "GKZ",
            "type": "Int64",
        },
        "VORWAHL_TELEFON": {
            "name": "Vorwahl_Telefon",
            "type": "object",
        },
        "TELEFON": {
            "name": "Telefon",
            "type": "object",
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
            "type": "object",
        },
        "HAUPTBRANCHE_08": {
            "name": "Hauptbranche",
            "type": "object",
        },
        "HAUPTBRANCHENTEXT_08": {
            "name": "Hauptbranchentext",
            "type": "object",
        },
        "NEBENBRANCHE_08": {
            "name": "Nebenbranche",
            "type": "object",
        },
        "ANZAHL_NIEDERLASSUNGEN": {
            "name": "Anzahl_Niederlassungen",
            "type": "object",
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
            "type": "object",
        },
        "HoechsteMutterNummer_0": {
            "name": "HNR",
            "type": "Float64",
        },
        "RECHTSFORM_TEXT": {
            "name": "Rechtsform",
            "type": "object",
        },
        "EHEMALIGER_FIRMENNAME": {
            "name": "Ehemaliger_Firmenname",
            "type": "object",
        },
        "HANDELSREGISTER": {
            "name": "Register",
            "type": "object",
        },
    },
    "BisnodePrimus": {
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
        },
        "GESCHLECHT_TEXT_1": {
            "name": "Geschlecht_Text_1",
            "type": "object",
        },
        "GESCHLECHT_TEXT_2": {
            "name": "Geschlecht_Text_2",
            "type": "object",
        },
        "GESCHLECHT_TEXT_3": {
            "name": "Geschlecht_Text_3",
            "type": "object",
        },
        "TITEL_1": {
            "name": "Titel_1",
            "type": "object",
        },
        "TITEL_2": {
            "name": "Titel_2",
            "type": "object",
        },
        "TITEL_3": {
            "name": "Titel_3",
            "type": "object",
        },
        "NAME_1": {
            "name": "Name_1",
            "type": "object",
        },
        "NAME_2": {
            "name": "Name_2",
            "type": "object",
        },
        "NAME_3": {
            "name": "Name_3",
            "type": "object",
        },
        "VORNAME_1": {
            "name": "Vorname_1",
            "type": "object",
        },
        "VORNAME_2": {
            "name": "Vorname_2",
            "type": "object",
        },
        "VORNAME_3": {
            "name": "Vorname_3",
            "type": "object",
        },
        "POSITION_TEXT_1": {
            "name": "Position_Text_1",
            "type": "object",
        },
        "POSITION_TEXT_2": {
            "name": "Position_Text_2",
            "type": "object",
        },
        "POSITION_TEXT_3": {
            "name": "Position_Text_3",
            "type": "object",
        },
        "INTERNET_ADRESSE": {
            "name": "Website",
            "type": "object",
        },
        "EMAIL": {
            "name": "Email",
            "type": "object",
        },
        "FIRMENZENTRALE_AUSLAND": {
            "name": "Firmenzentrale_Ausland",
            "type": "object",
        },
    },
    "BeDirect": {
        "BE_ID": {
            "name": "BED_ID",
            "type": "Int64",
        },
        "BE_FIRMENNAME_GESAMT": {
            "name": "Firmenname",
            "type": "object",
        },
        "BE_PLZ": {
            "name": "PLZ",
            "type": "object",
        },
        "BE_ORT": {
            "name": "Ort",
            "type": "object",
        },
        "BE_STRASSE": {
            "name": "Strasse",
            "type": "object",
        },
        "BE_HAUSNUMMER": {
            "name": "Hausnummer",
            "type": "object",
        },
        "BE_VORWAHL": {
            "name": "Vorwahl_Telefon",
            "type": "object",
        },
        "BE_RUFNUMMER": {
            "name": "Telefon",
            "type": "object",
        },
        "BE_E_MAIL": {
            "name": "Email",
            "type": "object",
        },
        "BE_HOMEPAGE": {
            "name": "Website",
            "type": "object",
        },
        "BE_PRIMAERBRANCHE": {
            "name": "Hauptbranche",
            "type": "object",
        },
        "BE_BRANCHE2": {
            "name": "Nebenbranche_1",
            "type": "object",
        },
        "BE_BRANCHE3": {
            "name": "Nebenbranche_2",
            "type": "object",
        },
        "BE_BRANCHE4": {
            "name": "Nebenbranche_3",
            "type": "object",
        },
        "BE_BRANCHE5": {
            "name": "Nebenbranche_4",
            "type": "object",
        },
        "BE_MITARBEITERSTAFFEL": {
            "name": "Beschaeftigte_Code",
            "type": "object",
        },
        "BE_ANZAHL_NL_FILIALEN": {
            "name": "Anzahl_Niederlassungen",
            "type": "object",
        },
        "HR_TYP": {
            "name": "Register_Type",
            "type": "object",
        },
        "HR_NUMMER": {
            "name": "Register_Nummer",
            "type": "object",
        },
        "BE_UMSATZSTAFFEL": {
            "name": "Umsatz_Code",
            "type": "object",
        },
        "TELEFON_SELECT": {
            "name": "Tel_Select",
            "type": "object",
        },
        "FLAG_QUALITAET_ORG": {
            "name": "Flag_Quality",
            "type": "object",
        },
        "BE_ANREDE": {
            "name": "Geschlecht_Text",
            "type": "object",
        },
        "BE_TITEL": {
            "name": "Titel",
            "type": "object",
        },
        "BE_VORNAME": {
            "name": "Vorname",
            "type": "object",
        },
        "BE_NACHNAME": {
            "name": "Name",
            "type": "object",
        },
        "BE_PREFIX": {
            "name": "Prefix_Name",
            "type": "object",
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
            "type": "object",
        },
    },
    "MapPlzBundesland": {
        "PLZ": {
            "name": "PLZ",
            "type": "object",
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": "object",
        },
    },
    "MapBedBranche": {
        "BRANCHE": {
            "name": "Hauptbranche",
            "type": "object",
        },
        "BRANCHENTEXT": {
            "name": "Hauptbranchentext",
            "type": "object",
        },
    },
    "BisnodeForBeD": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": "object",
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "object",
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": "object",
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": "object",
        },
    },
    "Industriescore": {
        "WZ8_H5_CODE": {
            "name": "Hauptbranche",
            "type": "object",
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
    "Prof. Dr. med.": "Prof. Dr.",
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
    "01": 0.130,
    "02": 0.170,
    "03": 0.270,
    "04": 0.630,
    "05": 1.935,
    "06": 5.720,
    "07": 30.000,
    "08": 78.316,
    "09": 670.832,
}

MAP_EMPL_MEDIAN = {
    "00": np.nan,
    "01": 2,
    "02": 6,
    "03": 12,
    "04": 26,
    "05": 61,
    "06": 121,
    "07": 251,
    "08": 590,
    "09": 1146,
    "10": 2320,
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
