# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 00 — Environment configuration
# 
# Every other notebook starts with `%run 00_config`. Nothing here is specific to
# a workspace, so the repository can be deployed unchanged into dev, test and
# production.
# 
# ## How it works
# 
# Item **names** are the contract across environments; **ids** are resolved at run
# time from the workspace the notebook is executing in. Deploying to a new
# workspace therefore needs no edits, provided the items keep their names.
# 
# | Value | Resolved from |
# |---|---|
# | `WS` | `notebookutils.runtime.context` — the current workspace |
# | `ES_ID` | Eventstream named `ES_NAME` |
# | `ES_SRC` | Custom endpoint source inside that Eventstream |
# | `KQL_URI` | Eventhouse `properties.queryServiceUri` |
# | `KQL_DB` | The Eventhouse's first KQL database |
# | `LH_ID` | Lakehouse named `LH_NAME` |
# 
# ## Credentials
# 
# `FIRMS_KEY`, `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` are set as literals
# in the credentials cell below, so every key lives in exactly one place.
# 
# > **These values are readable by anyone with access to this workspace or to the
# > Git repository it syncs with. Keep the repository private, and rotate any key
# > that reaches a public remote.**
# 
# To move them out of source later, set `KEYVAULT_URI`. When it is set the vault
# value wins and the literal is ignored, so no other notebook has to change.
# 
# OpenSky credentials are optional: blank means anonymous access, which is rate
# limited to roughly 400 credits/day and is enough at a 5-minute cadence.


# CELL ********************

# Environment contract: names, never ids.
# Change these only if the items are named differently in the target workspace.
ES_NAME        = "ES_Wildfire"
ES_SOURCE_NAME = "wildfire_ingest"    # custom endpoint inside the Eventstream
EH_NAME        = "EH_Wildfire"
LH_NAME        = "LH_WildFires"

# Outbound identification.
#
# The OpenStreetMap Overpass API requires clients to identify themselves with a
# descriptive User-Agent and a reachable contact, so operators can get in touch
# before throttling or blocking a heavy client. Library defaults such as
# "python-requests/2.x" are treated as anonymous scrapers and are commonly
# refused with HTTP 429 or 403. GDACS is sent the same header by convention.
#
# Use a monitored team alias, not a personal address: this file is committed.
APP_NAME      = "FabricWildfireDemo/1.0"
CONTACT_EMAIL = "wildfire-demo@contoso.com"

UA = {"User-Agent": f"{APP_NAME} (contact: {CONTACT_EMAIL})"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# CREDENTIALS - the only place API keys are defined.
#
# WARNING: these are readable by anyone with access to this workspace or to the
# Git repository it syncs with. Keep the repository private and rotate any key
# that reaches a public remote.
#
# To move them out of source, set KEYVAULT_URI below. When it is set the vault
# value wins and these literals are ignored.
# ---------------------------------------------------------------------------

# NASA FIRMS map key. Free from https://firms.modaps.eosdis.nasa.gov/api/area/
FIRMS_KEY = ""

# OpenSky Network OAuth2. Blank means anonymous access, which is rate limited
# to roughly 400 credits/day and is sufficient at a 5-minute cadence.
OPENSKY_CLIENT_ID     = ""
OPENSKY_CLIENT_SECRET = ""

# Optional Azure Key Vault override. Leave empty to use the literals above.
# The identity running the notebook needs "Key Vault Secrets User".
KEYVAULT_URI = ""
SECRET_NAMES = {
    "FIRMS_KEY":             "firms-map-key",
    "OPENSKY_CLIENT_ID":     "opensky-client-id",
    "OPENSKY_CLIENT_SECRET": "opensky-client-secret",
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve every environment-specific id at run time.
import notebookutils, requests

FABRIC_API = "https://api.fabric.microsoft.com"


def _token():
    return notebookutils.credentials.getToken(FABRIC_API)


def _get(path):
    r = requests.get(f"{FABRIC_API}/v1{path}",
                     headers={"Authorization": f"Bearer {_token()}"}, timeout=60)
    r.raise_for_status()
    return r.json()


def _current_workspace():
    ctx = notebookutils.runtime.context
    for key in ("currentWorkspaceId", "workspaceId"):
        if ctx.get(key):
            return ctx[key]
    raise RuntimeError(f"cannot determine workspace from context: {list(ctx)}")


def find_item(name, item_type):
    """Item id by display name. Names are stable across environments, ids are not."""
    items = _get(f"/workspaces/{WS}/items?type={item_type}")["value"]
    for it in items:
        if it["displayName"] == name:
            return it["id"]
    available = ", ".join(sorted(i["displayName"] for i in items)) or "(none)"
    raise LookupError(f"{item_type} named '{name}' not found. Present: {available}")


WS = _current_workspace()

# Eventstream and its custom endpoint source.
ES_ID = find_item(ES_NAME, "Eventstream")
_sources = _get(f"/workspaces/{WS}/eventstreams/{ES_ID}/topology").get("sources", [])
_match = ([s for s in _sources if s.get("name") == ES_SOURCE_NAME]
          or [s for s in _sources if s.get("type") == "CustomEndpoint"])
if not _match:
    raise LookupError(f"no custom endpoint source in Eventstream '{ES_NAME}'")
ES_SRC = _match[0]["id"]

# Eventhouse query URI and database name.
_eh = _get(f"/workspaces/{WS}/eventhouses/{find_item(EH_NAME, 'Eventhouse')}")
KQL_URI = _eh["properties"]["queryServiceUri"]
_dbs = _eh["properties"].get("databasesItemIds") or []
KQL_DB = (_get(f"/workspaces/{WS}/kqlDatabases/{_dbs[0]}")["displayName"]
          if _dbs else EH_NAME)

# Lakehouse.
LH_ID = find_item(LH_NAME, "Lakehouse")


def _from_vault(var_name, current):
    """Key Vault overrides the literal when KEYVAULT_URI is set."""
    if not KEYVAULT_URI:
        return current
    try:
        value = notebookutils.credentials.getSecret(
            KEYVAULT_URI, SECRET_NAMES[var_name])
        if value:
            return value
        print(f"  {var_name}: vault returned empty, keeping literal")
    except Exception as exc:
        print(f"  {var_name}: vault lookup failed ({exc}), keeping literal")
    return current


FIRMS_KEY = _from_vault("FIRMS_KEY", FIRMS_KEY)
OPENSKY_CLIENT_ID = _from_vault("OPENSKY_CLIENT_ID", OPENSKY_CLIENT_ID)
OPENSKY_CLIENT_SECRET = _from_vault("OPENSKY_CLIENT_SECRET", OPENSKY_CLIENT_SECRET)

_origin = "key vault" if KEYVAULT_URI else "config literal"
if "example.com" in CONTACT_EMAIL:
    print("  WARNING: CONTACT_EMAIL is still the placeholder. Overpass may "
          "throttle or block this client. Set it in 00_config.")
print("resolved configuration")
print(f"  workspace   {WS}")
print(f"  eventstream {ES_NAME} {ES_ID}")
print(f"  es source   {ES_SOURCE_NAME} {ES_SRC}")
print(f"  eventhouse  {KQL_DB} {KQL_URI}")
print(f"  lakehouse   {LH_NAME} {LH_ID}")
print(f"  credentials from {_origin}")
print(f"    firms key {'set' if FIRMS_KEY else 'MISSING'}")
print(f"    opensky   {'authenticated' if OPENSKY_CLIENT_ID else 'anonymous'}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
