import numpy as np
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType

COLUMN_DEFINITIONS = {
    "Bisnode": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": IntegerType(),
        },
        "STATUS": {
            "name": "Status",
            "type": StringType(),
        },
        "FIRMENNAME": {
            "name": "Firmenname",
            "type": StringType(),
        },
        "FIRMENTYP_LANGTEXT": {
            "name": "Firmentype",
            "type": StringType(),
        },
        "HANDELSNAME": {
            "name": "Handelsname",
            "type": StringType(),
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": IntegerType(),
        },
        "PLZ_STRASSE": {
            "name": "PLZ",
            "type": StringType(),
        },
        "ORT_STRASSE": {
            "name": "Ort",
            "type": StringType(),
        },
        "STRASSE": {
            "name": "Strasse",
            "type": StringType(),
        },
        "HAUSNUMMER": {
            "name": "Hausnummer",
            "type": StringType(),
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": StringType(),
        },
        "GKZ": {
            "name": "GKZ",
            "type": IntegerType(),
        },
        "VORWAHL_TELEFON": {
            "name": "Vorwahl_Telefon",
            "type": StringType(),
        },
        "TELEFON": {
            "name": "Telefon",
            "type": StringType(),
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": FloatType(),
        },
        "UMSATZ_JAHR": {
            "name": "Umsatz_Jahr",
            "type": FloatType(),
        },
        "SEGMENT_DICC": {
            "name": "Segment",
            "type": FloatType(),
        },
        "MARKETABLE": {
            "name": "Marketable",
            "type": StringType(),
        },
        "HAUPTBRANCHE_08": {
            "name": "Hauptbranche",
            "type": StringType(),
        },
        "HAUPTBRANCHENTEXT_08": {
            "name": "Hauptbranchentext",
            "type": StringType(),
        },
        "NEBENBRANCHE_08": {
            "name": "Nebenbranche",
            "type": StringType(),
        },
        "ANZAHL_NIEDERLASSUNGEN": {
            "name": "Anzahl_Niederlassungen",
            "type": IntegerType(),
        },
        "ANZAHL_KONZERNMITGLIEDER": {
            "name": "Anzahl_Konzernmitglieder",
            "type": IntegerType(),
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": FloatType(),
        },
        "DIREKTE_MUTTER_NUMMER": {
            "name": "Direkte_Mutter_Nummer",
            "type": DoubleType(),
        },
        "DIREKTE_MUTTER_LAND": {
            "name": "Direkte_Mutter_Land",
            "type": StringType(),
        },
        "HoechsteMutterNummer_0": {
            "name": "HNR",
            "type": DoubleType(),
        },
        "RECHTSFORM_TEXT": {
            "name": "Rechtsform",
            "type": StringType(),
        },
        "EHEMALIGER_FIRMENNAME": {
            "name": "Ehemaliger_Firmenname",
            "type": StringType(),
        },
        "HANDELSREGISTER": {
            "name": "Register",
            "type": StringType(),
        },
    },
    "BisnodePrimus": {
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": IntegerType(),
        },
        "GESCHLECHT_TEXT_1": {
            "name": "Geschlecht_Text_1",
            "type": StringType(),
        },
        "GESCHLECHT_TEXT_2": {
            "name": "Geschlecht_Text_2",
            "type": StringType(),
        },
        "GESCHLECHT_TEXT_3": {
            "name": "Geschlecht_Text_3",
            "type": StringType(),
        },
        "TITEL_1": {
            "name": "Titel_1",
            "type": StringType(),
        },
        "TITEL_2": {
            "name": "Titel_2",
            "type": StringType(),
        },
        "TITEL_3": {
            "name": "Titel_3",
            "type": StringType(),
        },
        "NAME_1": {
            "name": "Name_1",
            "type": StringType(),
        },
        "NAME_2": {
            "name": "Name_2",
            "type": StringType(),
        },
        "NAME_3": {
            "name": "Name_3",
            "type": StringType(),
        },
        "VORNAME_1": {
            "name": "Vorname_1",
            "type": StringType(),
        },
        "VORNAME_2": {
            "name": "Vorname_2",
            "type": StringType(),
        },
        "VORNAME_3": {
            "name": "Vorname_3",
            "type": StringType(),
        },
        "POSITION_TEXT_1": {
            "name": "Position_Text_1",
            "type": StringType(),
        },
        "POSITION_TEXT_2": {
            "name": "Position_Text_2",
            "type": StringType(),
        },
        "POSITION_TEXT_3": {
            "name": "Position_Text_3",
            "type": StringType(),
        },
        "INTERNET_ADRESSE": {
            "name": "Website",
            "type": StringType(),
        },
        "EMAIL": {
            "name": "Email",
            "type": StringType(),
        },
        "FIRMENZENTRALE_AUSLAND": {
            "name": "Firmenzentrale_Ausland",
            "type": StringType(),
        },
    },
    "BeDirect": {
        "BE_ID": {
            "name": "BED_ID",
            "type": IntegerType(),
        },
        "BE_FIRMENNAME_GESAMT": {
            "name": "Firmenname",
            "type": StringType(),
        },
        "BE_PLZ": {
            "name": "PLZ",
            "type": StringType(),
        },
        "BE_ORT": {
            "name": "Ort",
            "type": StringType(),
        },
        "BE_STRASSE": {
            "name": "Strasse",
            "type": StringType(),
        },
        "BE_HAUSNUMMER": {
            "name": "Hausnummer",
            "type": StringType(),
        },
        "BE_VORWAHL": {
            "name": "Vorwahl_Telefon",
            "type": StringType(),
        },
        "BE_RUFNUMMER": {
            "name": "Telefon",
            "type": StringType(),
        },
        "BE_E_MAIL": {
            "name": "Email",
            "type": StringType(),
        },
        "BE_HOMEPAGE": {
            "name": "Website",
            "type": StringType(),
        },
        "BE_PRIMAERBRANCHE": {
            "name": "Hauptbranche",
            "type": StringType(),
        },
        "BE_BRANCHE2": {
            "name": "Nebenbranche_1",
            "type": StringType(),
        },
        "BE_BRANCHE3": {
            "name": "Nebenbranche_2",
            "type": StringType(),
        },
        "BE_BRANCHE4": {
            "name": "Nebenbranche_3",
            "type": StringType(),
        },
        "BE_BRANCHE5": {
            "name": "Nebenbranche_4",
            "type": StringType(),
        },
        "BE_MITARBEITERSTAFFEL": {
            "name": "Beschaeftigte_Code",
            "type": StringType(),
        },
        "BE_ANZAHL_NL_FILIALEN": {
            "name": "Anzahl_Niederlassungen",
            "type": StringType(),
        },
        "HR_TYP": {
            "name": "Register_Type",
            "type": StringType(),
        },
        "HR_NUMMER": {
            "name": "Register_Nummer",
            "type": StringType(),
        },
        "BE_UMSATZSTAFFEL": {
            "name": "Umsatz_Code",
            "type": StringType(),
        },
        "TELEFON_SELECT": {
            "name": "Tel_Select",
            "type": StringType(),
        },
        "FLAG_QUALITAET_ORG": {
            "name": "Flag_Quality",
            "type": StringType(),
        },
        "BE_ANREDE": {
            "name": "Geschlecht_Text",
            "type": StringType(),
        },
        "BE_TITEL": {
            "name": "Titel",
            "type": StringType(),
        },
        "BE_VORNAME": {
            "name": "Vorname",
            "type": StringType(),
        },
        "BE_NACHNAME": {
            "name": "Name",
            "type": StringType(),
        },
        "BE_PREFIX": {
            "name": "Prefix_Name",
            "type": StringType(),
        },
        "BIPID_DIREKTE_MUTTER": {
            "name": "Direkte_Mutter_Nummer",
            "type": IntegerType(),
        },
        "BIPID_HOECHSTE_MUTTER": {
            "name": "HNR",
            "type": IntegerType(),
        },
        "BE_RECHTSFORM_ID": {
            "name": "Rechtsform",
            "type": StringType(),
        },
    },
    "MapPlzBundesland": {
        "PLZ": {
            "name": "PLZ",
            "type": StringType(),
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": StringType(),
        },
    },
    "MapBedBranche": {
        "BRANCHE": {
            "name": "Hauptbranche",
            "type": StringType(),
        },
        "BRANCHENTEXT": {
            "name": "Hauptbranchentext",
            "type": StringType(),
        },
    },
    "BisnodeForBeD": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": StringType(),
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": StringType(),
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": StringType(),
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": StringType(),
        },
    },
    "Industriescore": {
        "WZ8_H5_CODE": {
            "name": "Hauptbranche",
            "type": StringType(),
        },
        "INDUSTRY_SCORE": {
            "name": "Industry_Score",
            "type": IntegerType(),
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
