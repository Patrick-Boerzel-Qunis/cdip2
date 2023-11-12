import numpy as np

COLUMN_DEFINITIONS2 = {
    "Bisnode2": {
        "FIRMENNUMMER": {
            "name": "BisnodeID",
            "type": "Int64",
            "spark_type": "LongType",
        },
        "STATUS": {
            "name": "Status",
            "type": "object",
            "spark_type": "StringType",
        },
        "FIRMENNAME": {
            "name": "Firmenname",
            "type": "object",
            "spark_type": "StringType",
        },
        "FIRMENTYP_LANGTEXT": {
            "name": "Firmentype",
            "type": "object",
            "spark_type": "StringType",
        },
        "HANDELSNAME": {
            "name": "Handelsname",
            "type": "object",
            "spark_type": "StringType",
        },
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
            "spark_type": "LongType",
        },
        "PLZ_STRASSE": {
            "name": "PLZ",
            "type": "object",
            "spark_type": "StringType",
        },
        "ORT_STRASSE": {
            "name": "Ort",
            "type": "object",
            "spark_type": "StringType",
        },
        "STRASSE": {
            "name": "Strasse",
            "type": "object",
            "spark_type": "StringType",
        },
        "HAUSNUMMER": {
            "name": "Hausnummer",
            "type": "object",
            "spark_type": "StringType",
        },
        "BUNDESLAND": {
            "name": "Bundesland",
            "type": "object",
            "spark_type": "StringType",
        },
        "GKZ": {
            "name": "GKZ",
            "type": "Int64",
            "spark_type": "LongType",
        },
        "VORWAHL_TELEFON": {
            "name": "Vorwahl_Telefon",
            "type": "object",
            "spark_type": "StringType",
        },
        "TELEFON": {
            "name": "Telefon",
            "type": "object",
            "spark_type": "StringType",
        },
        "UMSATZ": {
            "name": "Umsatz",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "UMSATZ_JAHR": {
            "name": "Umsatz_Jahr",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "SEGMENT_DICC": {
            "name": "Segment",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "MARKETABLE": {
            "name": "Marketable",
            "type": "object",
            "spark_type": "StringType",
        },
        "HAUPTBRANCHE_08": {
            "name": "Hauptbranche",
            "type": "object",
            "spark_type": "StringType",
        },
        "HAUPTBRANCHENTEXT_08": {
            "name": "Hauptbranchentext",
            "type": "object",
            "spark_type": "StringType",
        },
        "NEBENBRANCHE_08": {
            "name": "Nebenbranche",
            "type": "object",
            "spark_type": "StringType",
        },
        "ANZAHL_NIEDERLASSUNGEN": {
            "name": "Anzahl_Niederlassungen",
            "type": "object",
            "spark_type": "StringType",
        },
        "ANZAHL_KONZERNMITGLIEDER": {
            "name": "Anzahl_Konzernmitglieder",
            "type": "Int64",
            "spark_type": "LongType",
        },
        "BESCHAEFTIGTE": {
            "name": "Beschaeftigte",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "DIREKTE_MUTTER_NUMMER": {
            "name": "Direkte_Mutter_Nummer",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "DIREKTE_MUTTER_LAND": {
            "name": "Direkte_Mutter_Land",
            "type": "object",
            "spark_type": "StringType",
        },
        "HoechsteMutterNummer_0": {
            "name": "HNR",
            "type": "Float64",
            "spark_type": "DoubleType",
        },
        "RECHTSFORM_TEXT": {
            "name": "Rechtsform",
            "type": "object",
            "spark_type": "StringType",
        },
        "EHEMALIGER_FIRMENNAME": {
            "name": "Ehemaliger_Firmenname",
            "type": "object",
            "spark_type": "StringType",
        },
        "HANDELSREGISTER": {
            "name": "Register",
            "type": "object",
            "spark_type": "StringType",
        },
    },
    "BisnodePrimus2": {
        "DNB_D_U_N_S_NUMMER": {
            "name": "DUNS_Nummer",
            "type": "Int64",
            "spark_type": "LongType",
        },
        "GESCHLECHT_TEXT_1": {
            "name": "Geschlecht_Text_1",
            "type": "object",
            "spark_type": "StringType",
        },
        "GESCHLECHT_TEXT_2": {
            "name": "Geschlecht_Text_2",
            "type": "object",
            "spark_type": "StringType",
        },
        "GESCHLECHT_TEXT_3": {
            "name": "Geschlecht_Text_3",
            "type": "object",
            "spark_type": "StringType",
        },
        "TITEL_1": {
            "name": "Titel_1",
            "type": "object",
            "spark_type": "StringType",
        },
        "TITEL_2": {
            "name": "Titel_2",
            "type": "object",
            "spark_type": "StringType",
        },
        "TITEL_3": {
            "name": "Titel_3",
            "type": "object",
            "spark_type": "StringType",
        },
        "NAME_1": {
            "name": "Name_1",
            "type": "object",
            "spark_type": "StringType",
        },
        "NAME_2": {
            "name": "Name_2",
            "type": "object",
            "spark_type": "StringType",
        },
        "NAME_3": {
            "name": "Name_3",
            "type": "object",
            "spark_type": "StringType",
        },
        "VORNAME_1": {
            "name": "Vorname_1",
            "type": "object",
            "spark_type": "StringType",
        },
        "VORNAME_2": {
            "name": "Vorname_2",
            "type": "object",
            "spark_type": "StringType",
        },
        "VORNAME_3": {
            "name": "Vorname_3",
            "type": "object",
            "spark_type": "StringType",
        },
        "POSITION_TEXT_1": {
            "name": "Position_Text_1",
            "type": "object",
            "spark_type": "StringType",
        },
        "POSITION_TEXT_2": {
            "name": "Position_Text_2",
            "type": "object",
            "spark_type": "StringType",
        },
        "POSITION_TEXT_3": {
            "name": "Position_Text_3",
            "type": "object",
            "spark_type": "StringType",
        },
        "INTERNET_ADRESSE": {
            "name": "Website",
            "type": "object",
            "spark_type": "StringType",
        },
        "EMAIL": {
            "name": "Email",
            "type": "object",
            "spark_type": "StringType",
        },
        "FIRMENZENTRALE_AUSLAND": {
            "name": "Firmenzentrale_Ausland",
            "type": "object",
            "spark_type": "StringType",
        },
    },
}