COMPANY_NAME_ATTRIBUTES = ["Firmenname", "Handelsname", "Rechtsform"]

COMPANY_ADDRESS_ATTRIBUTES = [
    "Address_ID",
    "PLZ",
    "Hausnummer",
    "Strasse",
    "Ort",
    "Bundesland",
    "GKZ",
    "Master_Marketable",
]

ASP_ATTRIBUTES = ["Geschlecht_Text", "Name", "Vorname", "Position_Text", "Titel"]

COMPANY_BRANCH_ATTRIBUTES = [
    "NACE_Code",
    "Branche",
    "Hauptbranche",
    "Hauptbranchentext",
    "Nebenbranche",
]

COMPANY_STATISTICS_ATTRIBUTES = [
    "Beschaeftigte",
    "Umsatz",
    "Segment",
    "Anzahl_Niederlassungen",
]

COMPANY_OTHER_ATTRIBUTES = ["Email", "Website", "Register"]

HNR_ATTRIBUTES = ["PVID_HNR", "HNR_not_present", "PVID_HNR_count"]

TELE_ATTRIBUTES = [
    "P1_TEL_KOMPLETT",
    "P2_TEL_KOMPLETT",
    "P1_TEL_TYP",
    "P2_TEL_TYP",
    "P1_TEL_MARKETABLE",
    "P2_TEL_MARKETABLE",
]

BEST_COMPANY_RAW_IDS = ["MAIN_DUNS_NUMMER", "MAIN_BED_ID"]


TRANSFORMED_ATTRIBUTES = [
    "PVID",  # index of golden potential.
    "GP_RAW_ID",  # dropped
    "DUNS_Nummer",  # Best in group selected as MAIN_DUNS_NUMMER
    "BED_ID",  # Best in PVID group selected as MAIN_BED_ID
    "Telefon_complete",  # transformed to P1/P2
    "Telefon_Type",  # transformed to P1/P2
    "Status",  # New definition
    "Created_Date",  # dropped
]


GOLDEN_POTENTIAL_ATTRIBUTES = (
    COMPANY_NAME_ATTRIBUTES
    + COMPANY_ADDRESS_ATTRIBUTES
    + ASP_ATTRIBUTES
    + COMPANY_BRANCH_ATTRIBUTES
    + COMPANY_STATISTICS_ATTRIBUTES
    + COMPANY_OTHER_ATTRIBUTES
    + HNR_ATTRIBUTES
    + TELE_ATTRIBUTES
    + BEST_COMPANY_RAW_IDS
    + ["Status"]
)
