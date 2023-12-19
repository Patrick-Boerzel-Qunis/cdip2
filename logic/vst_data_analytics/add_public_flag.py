from functools import reduce

import dask.dataframe as dd
import pandas as pd

from vst_data_analytics.utils import any_true, row_exists


def add_public_flag(
    df: dd.DataFrame,
    df_landkreis: dd.DataFrame,
    df_gemeinde: dd.DataFrame,
    df_public_stichworte: dd.DataFrame,
) -> dd.DataFrame:
    _keys = _get_keys(df)

    df = df.assign(
        in_stadtliste=is_in_stadtliste(
            pattern_stadt=_get_pattern_stadt(df_public_stichworte), **_keys
        )
    )

    df = df.assign(
        FLAG_PUBLIC=is_public(
            df_public_stichworte=df_public_stichworte,
            df_landkreis=df_landkreis,
            df_gemeinde=df_gemeinde,
            liste_regierungsbezirke=_get_liste_regierungsbezirke(),
            liste_parteien_lang=_get_liste_parteien_lang(df_public_stichworte),
            stichworte_public_case_true=_get_stichworte_public_case_true(),
            stichworte_public_case_false=_get_stichworte_public_case_false(),
            stichworte_nicht_public_case_true=_get_stichworte_nicht_public_case_true(),
            stichworte_nicht_public_case_false=_get_stichworte_nicht_public_case_false(),
            **_keys
        )
    )

    df = df.drop(columns=["in_stadtliste"])

    return df


def is_public(
    df_public_stichworte: dd.DataFrame,
    df_landkreis: dd.DataFrame,
    df_gemeinde: dd.DataFrame,
    liste_regierungsbezirke: list[str],
    liste_parteien_lang: str,
    stichworte_public_case_true: str,
    stichworte_public_case_false: str,
    stichworte_nicht_public_case_true: str,
    stichworte_nicht_public_case_false: str,
    firmennamen: str,
    rechtsform: str,
    branche: str,
    vorname: str,
    nachname: str,
    *args,
    **kwargs
):
    def func(df: dd.DataFrame) -> dd.DataFrame:
        return (
            public_includes(
                df,
                df_public_stichworte=df_public_stichworte,
                df_landkreis=df_landkreis,
                df_gemeinde=df_gemeinde,
                rechtsform=rechtsform,
                branche=branche,
                firmennamen=firmennamen,
                liste_regierungsbezirke=liste_regierungsbezirke,
                stichworte_public_case_true=stichworte_public_case_true,
                stichworte_public_case_false=stichworte_public_case_false,
            )
            & ~public_excludes(
                df,
                rechtsform=rechtsform,
                firmennamen=firmennamen,
                vorname=vorname,
                nachname=nachname,
                stichworte_nicht_public_case_true=stichworte_nicht_public_case_true,
                stichworte_nicht_public_case_false=stichworte_nicht_public_case_false,
                liste_parteien_lang=liste_parteien_lang,
            )
        ) | public_final_includes(df, firmennamen=firmennamen)

    return func


def public_includes(
    df: dd.DataFrame,
    df_public_stichworte: dd.DataFrame,
    df_landkreis: dd.DataFrame,
    df_gemeinde: dd.DataFrame,
    rechtsform: str,
    branche: str,
    firmennamen: list[str],
    liste_regierungsbezirke: list[str],
    stichworte_public_case_true: str,
    stichworte_public_case_false: str,
) -> dd.Series:
    public_types = [
        is_rechtsform(df, col_name=rechtsform),
        is_kommunal_verwaltung(df, col_name=rechtsform),
        col_starts_with(df, col_name=branche, prefix="8411"),
        is_staedte_stichwort(
            df, col_name=firmennamen[0], df_public_stichworte=df_public_stichworte
        ),
        are_any_landkreis(df, col_names=firmennamen, df_landkreis=df_landkreis),
        are_any_bezirk(
            df, col_names=firmennamen, liste_regierungsbezirke=liste_regierungsbezirke
        ),
        are_any_gemeinde(df, col_names=firmennamen, df_gemeinde=df_gemeinde),
        are_any_in_stichworte(
            df,
            col_names=firmennamen,
            stichworte_public_case_true=stichworte_public_case_true,
            stichworte_public_case_false=stichworte_public_case_false,
        ),
        are_any_agentur_arbeit(df, col_names=firmennamen),
        are_any_staatlich(df, col_names=firmennamen),
    ]
    return reduce(any_true, public_types)


def are_any_staatlich(df: dd.DataFrame, col_names: list[str]) -> dd.Series:
    return reduce(
        any_true, [is_staatlich(df, col_name=col_names) for col_names in col_names]
    )


def is_staatlich(df: dd.DataFrame, col_name: str) -> dd.Series:
    starts_with_staatlich = df[col_name].str.startswith("Staatl.") | df[
        col_name
    ].str.startswith("staatl.")
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
    staatlich_anerkannt = reduce(
        any_true,
        [
            df[col_name].str.startswith(identifier)
            for identifier in anerkannt_identifier
        ],
    )
    return row_exists(df, col_name) & starts_with_staatlich & ~staatlich_anerkannt


def are_any_agentur_arbeit(df: dd.DataFrame, col_names: list[str]) -> dd.Series:
    return reduce(
        any_true, [is_agentur_arbeit(df, col_name=col_name) for col_name in col_names]
    )


def is_agentur_arbeit(df: dd.DataFrame, col_name: str) -> dd.Series:
    is_agentur_arbeit = df[col_name].str.contains("Agentur für Arbeit") & ~df[
        col_name
    ].str.contains("Agentur für Arbeits")
    is_a_oe_r = df[col_name].str.contains("A.ö.R.", case=False, regex=False)
    return row_exists(df, col_name) & (is_agentur_arbeit | is_a_oe_r)


def are_any_in_stichworte(
    df: dd.DataFrame,
    col_names: list[str],
    stichworte_public_case_true: str,
    stichworte_public_case_false: str,
) -> dd.Series:
    return reduce(
        any_true,
        [
            is_in_stichworte(
                df,
                col_name=col_name,
                stichworte_public_case_true=stichworte_public_case_true,
                stichworte_public_case_false=stichworte_public_case_false,
            )
            for col_name in col_names
        ],
    )


def is_in_stichworte(
    df: dd.DataFrame,
    col_name: str,
    stichworte_public_case_true: str,
    stichworte_public_case_false: str,
) -> dd.Series:
    case_sensitive = row_exists(df, col_name) & df[col_name].str.contains(
        stichworte_public_case_true
    )
    case_insensitive = row_exists(df, col_name) & df[col_name].str.contains(
        stichworte_public_case_false, case=False
    )
    return case_sensitive | case_insensitive


def are_any_gemeinde(
    df: dd.DataFrame, col_names: list[str], df_gemeinde: dd.DataFrame
) -> dd.Series:
    df_gemeinde = df_gemeinde.compute()
    return reduce(
        any_true,
        [
            is_gemeinde(df, col_name=col_name, df_gemeinde=df_gemeinde)
            for col_name in col_names
        ],
    )


def is_gemeinde(
    df: dd.DataFrame, col_name: str, df_gemeinde: pd.DataFrame
) -> dd.Series:
    is_gemeinde = df[col_name].isin("Gemeinde " + df_gemeinde["Gemeinde"])
    return row_exists(df, col_name) & is_gemeinde


def are_any_bezirk(
    df: dd.DataFrame, col_names: list[str], liste_regierungsbezirke: list[str]
) -> dd.Series:
    return reduce(
        any_true,
        [
            is_bezirk(
                df,
                col_name=col_name,
                liste_regierungsbezirke=liste_regierungsbezirke,
            )
            for col_name in col_names
        ],
    )


def is_bezirk(
    df: dd.DataFrame, col_name: str, liste_regierungsbezirke: list[str]
) -> dd.Series:
    is_bezirk = df[col_name].isin(
        ["Regierungsbezirk " + liste for liste in liste_regierungsbezirke]
    )
    return row_exists(df, col_name) & is_bezirk


def are_any_landkreis(
    df: dd.DataFrame, col_names: list[str], df_landkreis: dd.DataFrame
) -> dd.Series:
    df_landkreis = df_landkreis.compute()
    return reduce(
        any_true,
        [
            is_landkreis(df, col_name=col_name, df_landkreis=df_landkreis)
            for col_name in col_names
        ],
    )


def is_landkreis(
    df: dd.DataFrame, col_name: str, df_landkreis: pd.DataFrame
) -> dd.Series:
    is_landkreis = (
        df[col_name].isin("Kreis " + df_landkreis["Landkreis"])
        | df[col_name].isin("Landkreis " + df_landkreis["Landkreis"])
        | df[col_name].isin(
            df_landkreis["Landkreis"][df_landkreis["Landkreis"].str.endswith("Kreis")]
        )
    )
    return row_exists(df, col_name) & is_landkreis


def is_staedte_stichwort(
    df: dd.DataFrame, col_name: str, df_public_stichworte: dd.DataFrame
) -> dd.Series:
    df_public_stichworte = df_public_stichworte.compute()
    return (
        df[col_name].isin(df_public_stichworte["PUBLIC_STAEDTE"])
        | df[col_name].isin("Stadt" + df_public_stichworte["PUBLIC_STAEDTE"])
        | df[col_name].isin("Kreisstadt" + df_public_stichworte["PUBLIC_STAEDTE"])
        | df[col_name].isin("Kreisfreie Stadt" + df_public_stichworte["PUBLIC_STAEDTE"])
    )


def col_starts_with(df: dd.DataFrame, col_name: str, prefix: str) -> dd.Series:
    return row_exists(df, col_name) & df[col_name].str.startswith(prefix)


def is_kommunal_verwaltung(df: dd.DataFrame, col_name: str) -> dd.Series:
    return df[col_name].isin(["Kommunal-/Gemeindeverwaltung"]) & df["in_stadtliste"]


def is_rechtsform(df: dd.DataFrame, col_name: str) -> dd.Series:
    return df[col_name].isin(
        [
            "Anstalt öffentlichen Rechts",
            "Gebietskörperschaft des Landes",
            "Körperschaft öffentlichen Rechts",
            "Stiftung des öffentlichen Rechts",
        ]
    )


def public_excludes(
    df: dd.DataFrame,
    rechtsform: str,
    firmennamen: list[str],
    vorname: str,
    nachname: str,
    stichworte_nicht_public_case_true: str,
    stichworte_nicht_public_case_false: str,
    liste_parteien_lang: str,
) -> dd.Series:
    non_public_types = [
        is_verein_or_kirchlich(df, col_name=rechtsform),
        are_any_no_public_stichwort(
            df,
            col_names=firmennamen,
            stichworte_nicht_public_case_true=stichworte_nicht_public_case_true,
            stichworte_nicht_public_case_false=stichworte_nicht_public_case_false,
        ),
        are_any_stiftung(df, col_names=firmennamen),
        are_any_partner(df, col_names=firmennamen, vorname=vorname, nachname=nachname),
        are_any_partei(
            df, col_names=firmennamen, liste_parteien_lang=liste_parteien_lang
        ),
    ]
    return reduce(any_true, non_public_types)


def are_any_partei(
    df: dd.DataFrame, col_names: list[str], liste_parteien_lang: str
) -> dd.Series:
    return reduce(
        any_true,
        [
            is_partei(df, col_name=col_name, liste_parteien_lang=liste_parteien_lang)
            for col_name in col_names
        ],
    )


def is_partei(df: dd.DataFrame, col_name: str, liste_parteien_lang: str) -> dd.Series:
    is_in_parteien_list = df[col_name].str.contains(liste_parteien_lang)
    return row_exists(df, col_name) & is_in_parteien_list


def are_any_partner(
    df: dd.DataFrame, col_names: list[str], vorname: str, nachname: str
) -> dd.Series:
    return reduce(
        any_true,
        [
            is_partner(df, col_name=col_name, vorname=vorname, nachname=nachname)
            for col_name in col_names
        ],
    )


def is_partner(
    df: dd.DataFrame, col_name: str, vorname: str, nachname: str
) -> dd.Series:
    is_firmenname = (
        (df[col_name] == (df[vorname] + " " + df[nachname]))
        | (df[col_name] == (df[nachname] + ", " + df[vorname]))
        | (
            df[col_name]
            .str.replace("med.", "", regex=False)
            .str.replace("Dr.", "", regex=False)
            .str.replace("Prof.", "", regex=False)
            .str.lstrip(" ")
            .str.rstrip(" ")
            == (df[vorname] + " " + df[nachname])
        )
        | (
            df[col_name]
            .str.replace("med.", "", regex=False)
            .str.replace("Dr.", "", regex=False)
            .str.replace("Prof.", "", regex=False)
            .str.lstrip(" ")
            .str.rstrip(" ")
            == (df[nachname] + ", " + df[vorname])
        )
    )
    return (
        row_exists(df, col_name)
        & row_exists(df, vorname)
        & row_exists(df, nachname)
        & is_firmenname
    )


def are_any_stiftung(df: dd.DataFrame, col_names: list[str]) -> dd.Series:
    return reduce(
        any_true, [is_stiftung(df, col_name=col_name) for col_name in col_names]
    )


def is_stiftung(df: dd.DataFrame, col_name: str) -> dd.Series:
    is_stiftung = df[col_name].str.contains("Stiftung")
    is_public_stiftung = df[col_name].str.contains("Stiftung des öffentlichen Rechts")
    return row_exists(df, col_name) & is_stiftung & ~is_public_stiftung


def are_any_no_public_stichwort(
    df: dd.DataFrame,
    col_names: list[str],
    stichworte_nicht_public_case_true: str,
    stichworte_nicht_public_case_false: str,
) -> dd.Series:
    return reduce(
        any_true,
        [
            is_in_no_stichworte(
                df,
                col_name=col_name,
                stichworte_nicht_public_case_true=stichworte_nicht_public_case_true,
                stichworte_nicht_public_case_false=stichworte_nicht_public_case_false,
            )
            for col_name in col_names
        ],
    )


def is_in_no_stichworte(
    df: dd.DataFrame,
    col_name: str,
    stichworte_nicht_public_case_true: str,
    stichworte_nicht_public_case_false: str,
) -> dd.Series:
    case_sensitive = row_exists(df, col_name) & df[col_name].str.contains(
        stichworte_nicht_public_case_true
    )
    case_insensitive = row_exists(df, col_name) & df[col_name].str.contains(
        stichworte_nicht_public_case_false, case=False
    )
    is_ev = row_exists(df, col_name) & (
        df[col_name].str.endswith("e.V")
        | df[col_name].str.contains("e.v.", case=False, regex=False)
    )
    return case_sensitive | case_insensitive | is_ev


def is_verein_or_kirchlich(df: dd.DataFrame, col_name: str) -> dd.Series:
    return df[col_name].isin(["eingetragener Verein", "kirchliche Institution"])


def public_final_includes(df: dd.DataFrame, firmennamen: list[str]) -> dd.Series:
    public_types = [are_any_staedtisch(df, col_names=firmennamen)]
    return reduce(any_true, public_types)


def are_any_staedtisch(df: dd.DataFrame, col_names: list[str]) -> dd.Series:
    return reduce(
        any_true, [is_staedtisch(df, col_name=col_name) for col_name in col_names]
    )


def is_staedtisch(df: dd.DataFrame, col_name: str) -> dd.Series:
    staedtisch_prefixes = ["Städt.", "städt.", "Städtisch", "städtisch"]
    is_staedtisch = reduce(
        any_true,
        [df[col_name].str.startswith(prefix) for prefix in staedtisch_prefixes],
    )
    return row_exists(df, col_name) & is_staedtisch


def is_in_stadtliste(pattern: str, col_names: list[str], *args, **kwargs):
    def func(df: dd.DataFrame) -> dd.DataFrame:
        return reduce(
            any_true,
            [
                _is_stadt_pattern(df, pattern=pattern, col_name=col_name)
                for col_name in col_names
            ],
        )

    return func


def _is_col_stadt_pattern(df: dd.DataFrame, pattern: str, col_name: str) -> dd.Series:
    return df[col_name].str.contains(pattern)


def _is_stadt_pattern(df: dd.DataFrame, pattern: str, col_name: str) -> dd.Series:
    return row_exists(df, col_name) & _is_col_stadt_pattern(
        df, pattern=pattern, col_name=col_name
    )


def _get_stichworte_nicht_public_case_false() -> str:
    STICHWORTE_NICHT_PUBLIC_CASE_FALSE = [
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
    return "|".join(STICHWORTE_NICHT_PUBLIC_CASE_FALSE)


def _get_stichworte_nicht_public_case_true() -> str:
    STICHWORTE_NICHT_PUBLIC_CASE_TRUE = [
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
    return "|".join(STICHWORTE_NICHT_PUBLIC_CASE_TRUE)


def _get_stichworte_public_case_false() -> str:
    STICHWORTE_PUBLIC_CASE_FALSE = ["aör", "justizkasse", "gerichtshof"]
    return "|".join(STICHWORTE_PUBLIC_CASE_FALSE)


def _get_stichworte_public_case_true() -> str:
    STICHWORTE_PUBLIC_CASE_TRUE = [
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
    return "|".join(STICHWORTE_PUBLIC_CASE_TRUE)


def _get_liste_parteien_lang(df: dd.DataFrame) -> str:
    return "|".join(
        df["PUBLIC_PARTEIEN_LANG"][
            (df["PUBLIC_PARTEIEN_LANG"] != "")
            & (df["PUBLIC_PARTEIEN_LANG"] != "mut")
            & (df["PUBLIC_PARTEIEN_LANG"] != "Zukunft.")
        ].dropna()
    )


def _get_liste_regierungsbezirke() -> list[str]:
    return [
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


def _get_pattern_stadt(df: dd.DataFrame) -> str:
    return "|".join(df["PUBLIC_STAEDTE"][df["PUBLIC_STAEDTE"] != ""])


def _get_keys(
    df: dd.DataFrame, col_name: str = "Firmenname"
) -> dict[str, str | list[str]]:
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
    return INTERIM_KEYS if col_name in df.columns else FINAL_KEYS
