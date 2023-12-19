# Databricks notebook source
import sys
from functools import reduce

# COMMAND ----------

import dask
import dask.dataframe as dd
import numpy as np

# COMMAND ----------

sys.path.append(f"../logic")

# COMMAND ----------

account_name = "cdip0dev0std"
account_key = dbutils.secrets.get(scope="cdip-scope", key="dask_key")

# COMMAND ----------

LANDING_OUT_DIR = "data_pipeline/data_abraham_pipeline"
TARGET_TABLE = "t_finalization"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load data

# COMMAND ----------

storage_options = {"account_name": account_name, "account_key": account_key}

# COMMAND ----------

# main
aufb_path = f"az://landing/{LANDING_OUT_DIR}/t_entflechtung/*.parquet"
df: dd.DataFrame = dd.read_parquet(
    path=aufb_path,
    storage_options=storage_options, 
    engine="pyarrow",
)


# COMMAND ----------

# landkreise
df_landkreis: dd.DataFrame = dd.read_parquet(
    path="az://landing/data/public/DATALAB.F_LANDKREISE.parquet",
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

# gemeinde
df_gemeinde: dd.DataFrame = dd.read_parquet(
    path="az://landing/data/public/DATALAB.F_GEMEINDEN.parquet",
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

# public stichworte
df_public_stichworte: dd.DataFrame = dd.read_parquet(
    path="az://landing/data/public/DATALAB.PUBLIC_STICHWORTE.parquet",
    storage_options=storage_options, 
    engine="pyarrow",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Processing

# COMMAND ----------

# AUR21: "Standartize Hausnummer"
df.Hausnummer = df["Hausnummer"].str.replace(" ", "").str.casefold()

# AUR22: "if Status = 0 then Master_Marketable = 0"
df["Master_Marketable"] = df["Master_Marketable"] & df["Status"]

# AAR06: "Adding dummy fields"
df["Flag_Blacklist"] = False
df["FLAG_BESTANDSKUNDE"] = True
# df["FLAG_PUBLIC"] = True
df["FLAG_GUELTIG"] = 0

# AAR07: "Trasseninformationen Adding dummy fields"
# AAR08: "Adding dummy fields"
"""
Bestandskundenabgleich

Den Schlüssel für alle Informationen zu Bestandskunden stellt die Zentrale Versatel ID (ZVID) dar. Diese ID wird innerhalb des Master Data
Managements für jeden Eintrag vergeben, der im Bestandskunden-CRM Remedy oder im Vertriebs CRM MSD enthalten ist. Ausgehend von einer
bekannten ZVID lassen sich via Oracle DB beliebige weitere Detailinformationen ermitteln und anspielen.
Um zu ermitteln, ob ein CDIP Potential bereits mit ZVID im MDM gelistet ist, ist ein Adressabgleich erforderlich. Der DQ-Server stellt dafür einen erprobten
Workflow bereit, theoretisch kann der Abgleich aber über jeden Matching-Algorithmus erfolgen.
"""
for col in [
    "AB_CLUSTER",
    "AB_MUFFEN",
    "AB_TRASSEN_VERTRIEB",
    "AB_PLANTRASSEN_NICHT_WEBOM",
    "AB_PLANTRASSEN_NORMAL",
    "AB_PLANTRASSEN_5G",
    "AB_TRASSEN_ALL",
    "AB_TRASSEN_FREMD",
    "AB_TRASSEN_FREMD_BT",
    "AB_TRASSEN_FREMD_OHNE_BT",
    "AB_TRASSEN_OWN",
    "AB_TRASSEN_OWN_BT",
    "AB_TRASSEN_OWN_OHNE_BT",
    "PLZ_GEO",
    "ORT_GEO",
    "STRASSE_HNR_GEO",
    "X",
    "Y",
    # AAR08
    'Zentrale_Versatel_ID',
    'OMI_Firmenname',
    'OMI_Rechtsform',
    'OMI_Zahlen',
    'OMI_Service',
    'OMI_Verwaltung',
    'OMI_Holding',
    'OMI_Vertriebs', 
    'OMI_Beteiligungs',
    'OMI_Himmelsrichtung',
    'OMI_Grundstueck',
    'OMI_Bundesland',
    'OMI_Kirchengemeinde',
    # MVC
    'BLACKLIST_KRITERIUM',
    'ETL_TIMESTAMP',
    'FLAG_STRASSENSEITE',
    'FIBRE_SCORE',
]:
    df[col] = None

for col in [
    "OWN_LR_PEC",
    "LR_TYP",
    "LR_DAT",
    "FREMD_LR_PEC",
    "AGS",
    "GEN",
    "BEZ",
    "CLUSTER_ID",
]:
    df[col] = ""


# COMMAND ----------

# rename and select data
df = df.rename(
    columns={
        "Zentrale_Versatel_ID": "ZVID",
        "Handelsname": "DNB_HANDELSNAME",
        "Website": "INTERNETADRESSE",
        "Nebenbranche":"NEBENBRANCHEN",
        "Hauptbranche": "HAUPTBRANCHE_08",
        "Hauptbranchentext": "HAUPTBRANCHENTEXT_08",
        "Umsatz": "UMSATZ_MIO_EUR",
        "Segment": "KUNDENSEGMENT",
        "Register": "HANDELSREGISTER",
        "Rechtsform": "RECHTSFORM_TEXT",
    }
)
df = df.rename(columns={col: col.upper() for col in df.columns})

# COMMAND ----------

df_landkreis = df_landkreis.rename(columns={"LANDKREIS": "Landkreis"})

# COMMAND ----------

df_gemeinde = df_gemeinde.rename(columns={"GEMEINDE": "Gemeinde"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Add Public flag

# COMMAND ----------

INTERIM_KEYS = {
    "firmennamen": ["Firmenname", "Handelsname"],
    "rechtsform": "Rechtsform",
    "branche": "Hauptbranche",
    "vorname": "Vorname",
    "nachname": "Name",
}
FINAL_KEYS = {
    "firmennamen": ["FIRMENNAME", "DNB_HANDELSNAME"],
    "rechtsform": "RECHTSFORM_TEXT",
    "branche": "HAUPTBRANCHE_08",
    "vorname": "VORNAME",
    "nachname": "NAME",
}

# COMMAND ----------

_keys = INTERIM_KEYS if "Firmenname" in df.columns else FINAL_KEYS

# COMMAND ----------

pattern_stadt = "|".join(
    df_public_stichworte["PUBLIC_STAEDTE"][
        df_public_stichworte["PUBLIC_STAEDTE"] != ""
    ]
)

liste_parteien_lang = "|".join(
    df_public_stichworte["PUBLIC_PARTEIEN_LANG"][
        (df_public_stichworte["PUBLIC_PARTEIEN_LANG"] != "")
        & (df_public_stichworte["PUBLIC_PARTEIEN_LANG"] != "mut")
        & (df_public_stichworte["PUBLIC_PARTEIEN_LANG"] != "Zukunft.")
    ].dropna()
)

liste_regierungsbezirke = [
    "Freiburg",
    "Karlsruhe",
    "Stuttgart",
    "Tübingen",
    "Oberbayern",
    "Niederbayern",
    "Oberfranken",
    "Mittelfranken",
    "Unterfranken",
    "Oberpfalz",
    "Schwaben",
    "Darmstadt",
    "Gießen",
    "Kassel",
    "Arnsberg",
    "Detmold",
    "Düsseldorf",
    "Köln",
    "Münster",
]

stichworte_public_case_true = "|".join(
    [
        "Amt für",
        "Amtsgericht",
        "Anstalt",
        "Anwaltsgerichtshof",
        "Arbeitsamt",
        "Arbeitsgericht",
        "Auswärtiges Amt",
        "Behörde",
        "Bereitschaftspolizei",
        "Berufsschule",
        "Bezirk",
        "Bundes",
        "Bundesagentur",
        "Bundesanstalt",
        "Bundesbehörde",
        "Bundesgericht",
        "Bundesgerichtshof",
        "Bundesland",
        "Bundesministerium",
        "Bundesministerium der Finanzen",
        "Bundesministerium der Justiz und für Verbraucherschutz",
        "Bundesministerium der Verteidigung",
        "Bundesministerium des Innern, für Bau und Heimat",
        "Bundesministerium für Arbeit und Soziales",
        "Bundesministerium für Bildung und Forschung",
        "Bundesministerium für Ernährung und Landwirtschaft",
        "Bundesministerium für Familie, Senioren, Frauen und Jugend",
        "Bundesministerium für Gesundheit",
        "Bundesministerium für Umwelt, Naturschutz und nukleare Sicherheit",
        "Bundesministerium für Verkehr und digitale Infrastruktur",
        "Bundesministerium für Wirtschaft und Energie",
        "Bundesministerium für wirtschaftliche Zusammenarbeit und Entwicklung",
        "Bundessozialgericht",
        "Bundeswehr",
        "Deutsche Bahn Aktiengesellschaft",
        "Deutsche Bahn AG",
        "Deutsche Rentenversicherung",
        "Dienstleistungszentrum",
        "Eigenbetrieb",
        "Eigenbetrieb",
        "Entsorgung",
        "Fachhochschule",
        "Feuerwehr",
        "Finanzgericht",
        "Flughafen",
        "Freistaat",
        "Gemeindetag",
        "Gemeinschaftsschule",
        "Gericht",
        "Gesamtschule",
        "Grundschule",
        "Gymnasium",
        "Hansestadt",
        "Hauptschule",
        "Hauptstadt",
        "Hochschule",
        "IT-Dienstleistungszentrum",
        "JVA",
        "Job-Center",
        "Jugendstraf",
        "Justizvollzugsanstalt",
        "Kommunalbehörde",
        "Kommunale",
        "Kreisklinik",
        "Kreispolizei",
        "Kreistag",
        "Landesamt",
        "Landesamt",
        "Landesanstalt",
        "Landesarbeitsgericht",
        "Landesbehörde",
        "Landesbibliothek",
        "Landesbücherei",
        "Landeshauptstadt",
        "Landesjustiz",
        "Landesjustizverwaltung",
        "Landespolizeidirektion",
        "Landesschulbehörde",
        "Landessozialgericht",
        "Landesstraßenbaubehörde",
        "Landesverband",
        "Landeszentralbank",
        "Landgericht",
        "Landkreis",
        "Landkreistag",
        "Landratsamt",
        "Ministerium",
        "Mittelschule",
        "Oberlandesgericht",
        "Oberstufe",
        "Oberstufenzentrum",
        "Ortsgericht",
        "Polizei",
        "Präsidium",
        "Realschule",
        "Regelschule",
        "Regierungspräsidium",
        "Regionalverkehr",
        "Senat der",
        "Sekundarschule",
        "Senatsverwaltung",
        "Sozialgericht",
        "Stadtbibliothek",
        "Stadtbücherei",
        "Staatlich",
        "Staatsanwaltschaft",
        "Stadtwerk",
        "Städtetag",
        "Technische Universität",
        "Universität",
        "Universitätsklinikum",
        "Verbandsgemeinde",
        "Verfassungsgerichtshof",
        "Verkehrsbetrieb",
        "Verteidigung",
        "Verwaltungsgemeinschaft",
        "Verwaltungsgericht",
        "Vollstreckungsgericht",
        "Wasserschutzpolizei",
        "Wasserwerk",
        "Wetterdienst",
        "behörde",
        "kommunal",
        "kommunaler",
        "ministerium",
        "polizei",
        "städtisch",
        "städtische",
    ]
)

stichworte_public_case_false = "|".join(["aör", "justizkasse", "gerichtshof"])

stichworte_nicht_public_case_true = "|".join(
    [
        "Bundesarbeitsgemeinschaft",
        "Bundesliga",
        "Bundesverband",
        "Bundesverein",
        "Bundesvereinigung",
        "Bundesweit",
        "Innung",
        "Kirche",
        "Montessori",
        "Rudolf Steiner",
    ]
)

stichworte_nicht_public_case_false = "|".join(
    [
        "gewerkschaft",
        "handelskammer",
        "kinderkrippe",
        "kita",
        "kindertagesstätte",
        "kindergarten",
        "partei",
        "verein",
        "wirtschaftskammer",
    ]
)

# COMMAND ----------

def any_true(acc, cur):
    return acc | cur


def row_exists(row, name):
    return row[name].notnull()

# COMMAND ----------

def set_in_stadtliste(firmennamen, *args, **kwargs):
    def firmenname_is_stadt_pattern(row, firmenname):
        return row[firmenname].str.contains(pattern_stadt)
    def in_stadt_pattern(row, firmenname):
        return row_exists(row, firmenname) & firmenname_is_stadt_pattern(row, firmenname)
    
    def wrapper(row):
        return reduce(any_true, [in_stadt_pattern(row, firmenname) for firmenname in firmennamen])
    return wrapper
df = df.assign(in_stadtliste=set_in_stadtliste(**_keys))

# COMMAND ----------

def set_public_flag(df_public_stichworte, df_landkreis, df_gemeinde, liste_regierungsbezirke, liste_parteien_lang, stichworte_public_case_true, stichworte_public_case_false, stichworte_nicht_public_case_true, stichworte_nicht_public_case_false, firmennamen, rechtsform, branche, vorname, nachname, *args, **kwargs):
    def firmenname_is_landkreis(row, firmenname, landkreis):
        is_landkreis = row[firmenname].isin("Kreis " + landkreis["Landkreis"]) | row[firmenname].isin("Landkreis " + landkreis["Landkreis"]) | row[firmenname].isin(landkreis["Landkreis"][landkreis["Landkreis"].str.endswith("Kreis")])
        return row_exists(row, firmenname) & is_landkreis

    def firmenname_is_bezirk(row, firmenname):
        is_bezirk = row[firmenname].isin(["Regierungsbezirk " + liste for liste in liste_regierungsbezirke])
        return row_exists(row, firmenname) & is_bezirk
    
    def firmenname_is_gemeinde(row, firmenname, gemeinde):
        is_gemeinde = row[firmenname].isin("Gemeinde " + gemeinde["Gemeinde"])
        return row_exists(row, firmenname) & is_gemeinde
    
    def firmenname_is_in_stichworte(row, firmenname):
        case_sensitive = row_exists(row, firmenname) & row[firmenname].str.contains(stichworte_public_case_true)
        case_insensitive = row_exists(row, firmenname) & row[firmenname].str.contains(stichworte_public_case_false, case=False)
        return case_sensitive | case_insensitive
    
    def firmenname_is_agentur_arbeit(row, firmenname):
        is_agentur_arbeit = row[firmenname].str.contains("Agentur für Arbeit") & ~row[firmenname].str.contains("Agentur für Arbeits")
        is_a_oe_r = row[firmenname].str.contains("A.ö.R.", case=False, regex=False)
        return row_exists(row, firmenname) & (is_agentur_arbeit | is_a_oe_r)
    
    def firmenname_is_staatlich(row, firmenname):
        starts_with_staatlich = row[firmenname].str.startswith("Staatl.") | row[firmenname].str.startswith("staatl.")
        anerkannt_identifier = [
            "Staatl. anerkannt",
            "staatl. anerkannt",
            "Staatlich anerkannt",
            "staatlich anerkannt",
            "Staatl. geprüft",
            "staatl. geprüft",
            "Staatlich geprüft",
            "staatlich geprüft",
            "Staatl. angezeigt",
            "staatl. angezeigt",
            "Staatlich angezeigt",
            "staatlich angezeigt",
        ]
        staatlich_anerkannt = reduce(any_true, [row[firmenname].str.startswith(identifier) for identifier in anerkannt_identifier])
        return row_exists(row, firmenname) & starts_with_staatlich & ~staatlich_anerkannt
    
    def firmenname_is_in_no_stichworte(row, firmenname):
        case_sensitive = row_exists(row, firmenname) & row[firmenname].str.contains(stichworte_nicht_public_case_true)
        case_insensitive = row_exists(row, firmenname) & row[firmenname].str.contains(stichworte_nicht_public_case_false, case=False)
        is_ev = row_exists(row, firmenname) & (row[firmenname].str.endswith("e.V") | row[firmenname].str.contains("e.v.", case=False, regex=False))
        return case_sensitive | case_insensitive | is_ev
    
    def firmenname_is_stiftung(row, firmenname):
        is_stiftung = row[firmenname].str.contains("Stiftung")
        is_public_stiftung = row[firmenname].str.contains("Stiftung des öffentlichen Rechts")
        return row_exists(row, firmenname) & is_stiftung & ~is_public_stiftung
    
    def firmenname_is_partner(row, firmenname):
        is_firmenname = (row[firmenname] == (row[vorname] + " " + row[nachname])) | (row[firmenname] == (row[nachname] + ", " + row[vorname])) | (row[firmenname].str.replace("med.", "", regex=False).str.replace("Dr.", "", regex=False).str.replace("Prof.", "", regex=False).str.lstrip(" ").str.rstrip(" ") == (row[vorname] + " " + row[nachname])) | (row[firmenname].str.replace("med.", "", regex=False).str.replace("Dr.", "", regex=False).str.replace("Prof.", "", regex=False).str.lstrip(" ").str.rstrip(" ") == (row[nachname] + ", " + row[vorname]))
        return row_exists(row, firmenname) & row_exists(row, vorname) & row_exists(row, nachname) & is_firmenname
    
    def firmenname_is_partei(row, firmenname):
        is_in_parteien_list = row[firmenname].str.contains(liste_parteien_lang)
        return row_exists(row, firmenname) & is_in_parteien_list
    
    def firmenname_is_staedtisch(row, firmenname):
        staedtisch_prefixes = ["Städt.", "städt.", "Städtisch", "städtisch"]
        is_staedtisch = reduce(any_true, [row[firmenname].str.startswith(prefix) for prefix in staedtisch_prefixes])
        return row_exists(row, firmenname) & is_staedtisch
    
    def excludes(row):
        is_rechtsform = row[rechtsform].isin(["eingetragener Verein", "kirchliche Institution"])
        is_no_public_stichwort = reduce(any_true, [firmenname_is_in_no_stichworte(row, firmenname) for firmenname in firmennamen])
        is_stiftung = reduce(any_true, [firmenname_is_stiftung(row, firmenname) for firmenname in firmennamen])
        is_firmenname_partner = reduce(any_true, [firmenname_is_partner(row, firmenname) for firmenname in firmennamen])
        is_partei = reduce(any_true, [firmenname_is_partei(row, firmenname) for firmenname in firmennamen])
        return is_rechtsform | is_no_public_stichwort | is_stiftung | is_firmenname_partner | is_partei
        
    def final_includes(row):
        is_staedtisch = reduce(any_true, [firmenname_is_staedtisch(row, firmenname) for firmenname in firmennamen])
        return is_staedtisch

    def includes(row):
        public_stichworte = df_public_stichworte.compute()
        landkreis = df_landkreis.compute()
        gemeinde = df_gemeinde.compute()
        is_rechtsform = row[rechtsform].isin([
            "Anstalt öffentlichen Rechts",
            "Gebietskörperschaft des Landes",
            "Körperschaft öffentlichen Rechts",
            "Stiftung des öffentlichen Rechts",
        ])
        is_kommunal_verwaltung = row[rechtsform].isin(["Kommunal-/Gemeindeverwaltung"]) & row["in_stadtliste"]
        starts_with_8411 = row[branche].notnull() & row[branche].str.startswith("8411")
        is_staedte = row[firmennamen[0]].isin(public_stichworte["PUBLIC_STAEDTE"]) | row[firmennamen[0]].isin("Stadt" + public_stichworte["PUBLIC_STAEDTE"]) | row[firmennamen[0]].isin("Kreisstadt" + public_stichworte["PUBLIC_STAEDTE"]) | row[firmennamen[0]].isin("Kreisfreie Stadt" + public_stichworte["PUBLIC_STAEDTE"])
        is_landkreis = reduce(any_true, [firmenname_is_landkreis(row, firmenname, landkreis=landkreis) for firmenname in firmennamen])
        is_bezirk = reduce(any_true, [firmenname_is_bezirk(row, firmenname) for firmenname in firmennamen])
        is_gemeinde = reduce(any_true, [firmenname_is_gemeinde(row, firmenname, gemeinde=gemeinde) for firmenname in firmennamen])
        is_in_stichworte = reduce(any_true, [firmenname_is_in_stichworte(row, firmenname) for firmenname in firmennamen])
        is_agentur_arbeit = reduce(any_true, [firmenname_is_agentur_arbeit(row, firmenname) for firmenname in firmennamen])
        is_staatlich = reduce(any_true, [firmenname_is_staatlich(row, firmenname) for firmenname in firmennamen])
        
        return is_rechtsform | is_kommunal_verwaltung | starts_with_8411 | is_staedte | is_landkreis | is_bezirk | is_gemeinde | is_in_stichworte | is_agentur_arbeit | is_staatlich

    def wrapper(row):
        return (includes(row) & ~excludes(row)) | final_includes(row)

    return wrapper
    
# Set to overwrite df_tmp *after* development finished
df = df.assign(FLAG_PUBLIC=set_public_flag(df_public_stichworte=df_public_stichworte, df_landkreis=df_landkreis, df_gemeinde=df_gemeinde, liste_regierungsbezirke=liste_regierungsbezirke, liste_parteien_lang=liste_parteien_lang, stichworte_public_case_true=stichworte_public_case_true, stichworte_public_case_false=stichworte_public_case_false, stichworte_nicht_public_case_true=stichworte_nicht_public_case_true, stichworte_nicht_public_case_false=stichworte_nicht_public_case_false, **_keys))

# COMMAND ----------

# remove intermediate column 'in_stadtliste'
df = df.drop(columns=["in_stadtliste"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Finalization

# COMMAND ----------

df = df.assign(
    MVC_INAKTIV=lambda x: ~x.STATUS,
    MVC_DUPLIKAT_GEBAEUDE=lambda x: x.ENTFL_ADDRESS == "Nicht Bester am Standort",
    MVC_BESTANDSKUNDE=lambda x: x.FLAG_BESTANDSKUNDE
    if "FLAG_BESTANDSKUNDE" in x
    else False,
    MVC_BLACKLIST=lambda x: x.FLAG_BLACKLIST if "FLAG_BLACKLIST" in x else False,
    MVC_NON_MARKETABLE=lambda x: ~x.MASTER_MARKETABLE,
    MVC_UNGUELTIGE_TEL=lambda x: ~x.P1_TEL_TYP.isin(["Festnetz", "Fixed network"]),
    MVC_LE=lambda x: x.KUNDENSEGMENT == 1,
    MVC_PUBLIC=lambda x: x.FLAG_PUBLIC if "FLAG_PUBLIC" in x else False,
    MVC_UNTERNEHMENSZENTRALEN=lambda x: x.ENTFL_NATIONAL.isin(
        ["Einzelunternehmen", "Firmenzentrale"]
    ),
    MVC_NIEDERLASSUNGEN=lambda x: x.ENTFL_NATIONAL == "Niederlassung",
)

# COMMAND ----------

df = df[[
    "PVID",
    "ZVID",
    "MAIN_DUNS_NUMMER",
    "MAIN_BED_ID",
    "ADDRESS_ID",
    "FIRMENNAME",
    "DNB_HANDELSNAME",
    "STRASSE",
    "HAUSNUMMER",
    "PLZ",
    "ORT",
    "BUNDESLAND",
    "GKZ",
    "EMAIL",
    "INTERNETADRESSE",
    "P1_TEL_KOMPLETT",
    "P1_TEL_TYP",
    "P2_TEL_KOMPLETT",
    "P2_TEL_TYP",
    "GESCHLECHT_TEXT",
    "TITEL",
    "VORNAME",
    "NAME",
    "POSITION_TEXT",
    "NACE_CODE",
    "BRANCHE",
    "HAUPTBRANCHE_08",
    "HAUPTBRANCHENTEXT_08",
    "NEBENBRANCHEN",
    "BESCHAEFTIGTE",
    "UMSATZ_MIO_EUR",
    "KUNDENSEGMENT",
    "ANZAHL_NIEDERLASSUNGEN",
    "HANDELSREGISTER",
    "RECHTSFORM_TEXT",
    "HNR_AGG",
    "STATUS",
    "MASTER_MARKETABLE",
    "FLAG_BESTANDSKUNDE",
    "FLAG_BLACKLIST",
    "BLACKLIST_KRITERIUM",
    "FLAG_PUBLIC",
    "AB_CLUSTER",
    "AB_MUFFEN",
    "AB_TRASSEN_VERTRIEB",
    "AB_PLANTRASSEN_NICHT_WEBOM",
    "AB_PLANTRASSEN_NORMAL",
    "AB_PLANTRASSEN_5G",
    "AB_TRASSEN_ALL",
    "AB_TRASSEN_FREMD",
    "AB_TRASSEN_FREMD_BT",
    "AB_TRASSEN_FREMD_OHNE_BT",
    "AB_TRASSEN_OWN",
    "AB_TRASSEN_OWN_BT",
    "AB_TRASSEN_OWN_OHNE_BT",
    "ETL_TIMESTAMP",
    "X",
    "Y",
    "FLAG_STRASSENSEITE",
    "FIBRE_SCORE",
    "ENTFL_NATIONAL",
    "ENTFL_ADDRESS",
    "ENTFL_GROUP_ID",
    "MVC_INAKTIV",
    "MVC_DUPLIKAT_GEBAEUDE",
    "MVC_BESTANDSKUNDE",
    "MVC_BLACKLIST",
    "MVC_NON_MARKETABLE",
    "MVC_UNGUELTIGE_TEL",
    "MVC_LE",
    "MVC_PUBLIC",
    "MVC_UNTERNEHMENSZENTRALEN",
    "MVC_NIEDERLASSUNGEN",
    "OWN_LR_PEC",
    "FLAG_GUELTIG",
    "LR_DAT",
    "LR_TYP",
    "FREMD_LR_PEC",
    "AGS",
    "GEN",
    "BEZ",
    "CLUSTER_ID",
    "ADDRESS_KEY",
]]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to table

# COMMAND ----------

tmp_abfss_path = f"abfss://landing@cdip0dev0std.dfs.core.windows.net/{LANDING_OUT_DIR}/{TARGET_TABLE}"
dbutils.fs.rm(tmp_abfss_path, recurse=True)

# COMMAND ----------

dd.to_parquet(df=df,
              path=f"az://landing/{LANDING_OUT_DIR}/{TARGET_TABLE}/",
              write_index=False,
              overwrite = True,
              storage_options={'account_name': account_name,
                               'account_key': account_key}
              )

# COMMAND ----------

spark.read.format("parquet").load(tmp_abfss_path).write.mode("overwrite").option("overwriteSchema", "True").saveAsTable(f"`vtl-dev`.bronze.{TARGET_TABLE}")
