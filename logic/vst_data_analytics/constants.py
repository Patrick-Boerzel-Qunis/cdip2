ADDRESS_MASTER_URL = (
    "https://api.1und1.net/gateway/addressmaster/1.0/api/v1/read/multi/address"
)
ADDRESS_MASTER_HEADERS = {
    "x-Gateway-APIKey": "02bed0a4-eca5-4b16-bdf9-6264b0b6d291",
    "Content-Type": "application/json",
}
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
