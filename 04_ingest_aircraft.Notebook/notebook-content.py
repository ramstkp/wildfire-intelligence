# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## Before you run this notebook
# 
# Pulls live aircraft positions from the OpenSky Network, classifies Securite
# Civile firefighting callsigns, and publishes to `ES_Wildfire`, which routes into
# `bronze_aircraft_raw`. Runs every 5 minutes.
# 
# ### What to configure
# 
# Everything is in the **Configuration** cell, immediately below this one.
# 
# | Setting | Purpose | Notes |
# |---|---|---|
# | `OPENSKY_CLIENT_ID`, `OPENSKY_CLIENT_SECRET` | OpenSky OAuth2 credentials | **Optional.** See below |
# | `WS`, `ES_ID`, `ES_SRC` | Where to publish | See the table below |
# | `WEST, SOUTH, EAST, NORTH` | Area of interest | Bounding box for the state vector query |
# | `FIRE_PREFIXES` | Callsign prefixes treated as firefighting | Must stay aligned with `ParseAircraft()` in the Eventhouse |
# | `SC_BASES` | Securite Civile air bases | Used for proximity context |
# 
# ### Getting OpenSky credentials, if you want them
# 
# Anonymous access works and is what the notebook uses when the fields are left
# empty. Anonymous callers get roughly **400 credits per day**, which is adequate
# at a 5-minute cadence for one bounding box.
# 
# For a higher quota:
# 
# 1. Register at <https://opensky-network.org/>
# 2. Sign in and open **Account**.
# 3. Create an API client to obtain a client id and client secret.
# 4. Paste them into the configuration cell.
# 
# Registered users get a substantially larger allowance. If responses start coming
# back empty or HTTP 429, the quota is exhausted; either wait, lengthen the
# schedule interval, or add credentials.
# 
# ### Recommended: keep credentials out of the notebook
# 
# Any value in the configuration cell is stored in the notebook definition and is
# visible to everyone with workspace access. For anything sensitive, put it in
# Azure Key Vault and read it at run time instead:
# 
# ```python
# import notebookutils
# FIRMS_KEY = notebookutils.credentials.getSecret(
#     "https://<your-vault>.vault.azure.net/", "firms-map-key")
# ```
# 
# The identity running the notebook needs **Key Vault Secrets User** on the vault.
# Replacing the literal with this call is the only change required.
# 
# ### Where the Fabric values come from
# 
# All of these identify resources in this workspace. None of them are secrets.
# 
# | Setting | How to obtain it |
# |---|---|
# | `WS` | Workspace id. Open the workspace in Fabric and copy the GUID after `/groups/` in the browser address bar. |
# | `ES_ID` | Eventstream item id. Open `ES_Wildfire` and copy the GUID after `/eventstreams/`. |
# | `ES_SRC` | Custom endpoint source id inside the Eventstream. In the Eventstream editor select the `wildfire_ingest` source; the id appears in its details pane. Also returned by `GET /v1/workspaces/{WS}/eventstreams/{ES_ID}/topology` under `sources[].id`. |
# | `KQL_URI` | Eventhouse query URI. Open `EH_Wildfire`, use **Copy query URI**, or read `properties.queryServiceUri` from `GET /v1/workspaces/{WS}/eventhouses/{id}`. |
# | `KQL_DB` | KQL database name, `EH_Wildfire` here. |
# 
# Authentication to Fabric and the Eventhouse needs no configuration: the notebook
# calls `notebookutils.credentials.getToken(...)` and runs as the submitting user.
# 
# ### Checking it worked
# 
# The notebook prints how many aircraft were retrieved, how many matched
# firefighting prefixes, and how many events were published. Verify with
# `silver_aircraft` in the Eventhouse.
# 
# Being classified as firefighting means only that the callsign matched a prefix.
# It is not confirmation that an aircraft is tasked to an incident.


# MARKDOWN ********************

# # 04 — Aerial Firefighting Assets
# 
# Streams live ADS-B positions over France from OpenSky into
# `bronze_aircraft_raw`.
# 
# ## Securite Civile fleet
# 
# | Callsign | Aircraft | Role |
# |---|---|---|
# | `PELICAN` | Canadair CL-415 | Water bomber, scoops from lakes and sea |
# | `MILAN` | Dash-8 Q400MR | Retardant tanker |
# | `DRAGON` | EC145 / H145 | Rescue helicopter |
# | `BEECH` | King Air | Airborne observation and coordination |
# 
# The `PELICAN` fleet is why `ref_water_bodies` carries a `scoopable` flag: a
# CL-415 refills in about 12 seconds by skimming a lake, so the nearest
# scoopable water sets the turnaround time and therefore the sortie rate.
# 
# > Outside an active incident the fleet is usually on the ground and silent.
# > That is expected, not a fault.
# 
# **Schedule every 5 minutes** during fire season.


# CELL ********************

%run 00_config

# CELL ********************

# Configuration

# France: metropolitan + Corsica
WEST, SOUTH, EAST, NORTH = -5.5, 41.0, 10.0, 51.5

# OAuth2 is optional. Anonymous access works but is rate-limited to roughly
# 400 credits/day, which is fine at a 5-minute cadence.

# Securite Civile callsign prefixes. These map to the Eventhouse
# ParseAircraft() function, which sets aircraft_role and is_firefighting.
#   PELICAN — Canadair CL-415 water bomber, scoops from lakes and sea
#   MILAN   — Dash-8 Q400MR retardant tanker
#   DRAGON  — EC145 / H145 rescue helicopter
#   BEECH   — Beechcraft King Air, airborne observation and coordination
FIRE_PREFIXES = ("PELICAN", "MILAN", "DRAGON", "BEECH", "CANADAIR", "TANKER")

# Bases aeriennes de la Securite Civile
SC_BASES = {
    "Nimes-Garons":     (43.757, 4.416),
    "Marignane":        (43.437, 5.221),
    "Ajaccio":          (41.924, 8.803),
    "Bastia":           (42.552, 9.484),
    "Carcassonne":      (43.216, 2.306),
    "Bordeaux-Merignac":(44.828, -0.715),
}

# CELL ********************

# Eventstream publisher
#
# The azure-eventhub SDK is not in the Fabric runtime, so this uses the Event
# Hubs REST API. The Eventstream maps JSON to the destination table by column
# name, and bronze_aircraft_raw is (event:dynamic, ingest_ts:datetime), so the
# payload must be shaped {"event": {...}, "ingest_ts": ...}.
import requests, json, time, base64, hashlib, hmac, urllib.parse, notebookutils
from datetime import datetime, timezone

_conn_cache = {}

def _conn():
    if "v" not in _conn_cache:
        tok = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
        url = (f"https://api.fabric.microsoft.com/v1/workspaces/{WS}"
               f"/eventstreams/{ES_ID}/sources/{ES_SRC}/connection")
        r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=60)
        r.raise_for_status()
        j = r.json()
        p = {}
        for part in j["accessKeys"]["primaryConnectionString"].split(";"):
            if part:
                k, _, v = part.partition("="); p[k] = v
        _conn_cache["v"] = (j["fullyQualifiedNamespace"], j["eventHubName"],
                            p["SharedAccessKeyName"], p["SharedAccessKey"])
    return _conn_cache["v"]

def _sas(uri, key_name, key, ttl=3600):
    enc = urllib.parse.quote_plus(uri)
    expiry = str(int(time.time() + ttl))
    sig = base64.b64encode(
        hmac.new(key.encode(), (enc + "\n" + expiry).encode(), hashlib.sha256).digest())
    return (f"SharedAccessSignature sr={enc}&sig={urllib.parse.quote_plus(sig)}"
            f"&se={expiry}&skn={key_name}")

def publish(events, label, event_type, batch_size=200):
    if not events:
        print(f"{label}: nothing to publish"); return 0
    ns, hub, kn, key = _conn()
    uri = f"https://{ns}/{hub}"
    hdr = {"Authorization": _sas(uri, kn, key),
           "Content-Type": "application/vnd.microsoft.servicebus.json"}
    now_iso = datetime.now(timezone.utc).isoformat()
    sent = 0
    for i in range(0, len(events), batch_size):
        chunk = events[i:i + batch_size]
        body = json.dumps([{"Body": json.dumps(
            {"event_type": event_type, "event": e, "ingest_ts": now_iso},
            default=str)} for e in chunk])
        ok = False
        for attempt in range(4):
            r = requests.post(f"{uri}/messages?timeout=60&api-version=2014-01",
                              headers=hdr, data=body.encode("utf-8"), timeout=120)
            if r.status_code in (200, 201):
                sent += len(chunk); ok = True; break
            if r.status_code == 401:
                hdr["Authorization"] = _sas(uri, kn, key); continue
            time.sleep(2 * (attempt + 1))
        if not ok:
            print(f"  batch at {i} failed: {r.status_code} {r.text[:200]}")
    print(f"{label}: published {sent}/{len(events)}")
    return sent

# MARKDOWN ********************

# ## Fetch live traffic


# CELL ********************

# Pull live ADS-B state vectors over France
import requests, pandas as pd
from datetime import datetime, timezone

headers = {}
if OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET:
    tok = requests.post(
        "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
        data={"grant_type":"client_credentials",
              "client_id":OPENSKY_CLIENT_ID,
              "client_secret":OPENSKY_CLIENT_SECRET}, timeout=60)
    tok.raise_for_status()
    headers = {"Authorization": f"Bearer {tok.json()['access_token']}"}
    print("OpenSky: authenticated")
else:
    print("OpenSky: anonymous (rate-limited)")

r = requests.get("https://opensky-network.org/api/states/all",
                 params={"lamin":SOUTH, "lomin":WEST, "lamax":NORTH, "lomax":EAST},
                 headers=headers, timeout=120)
r.raise_for_status()
states = r.json().get("states") or []
print(f"live aircraft over France: {len(states)}")

COLS = ["icao24","callsign","origin_country","time_position","last_contact",
        "longitude","latitude","baro_altitude","on_ground","velocity",
        "true_track","vertical_rate","sensors","geo_altitude","squawk",
        "spi","position_source"]
ac = pd.DataFrame([s[:17] for s in states], columns=COLS)
ac["callsign"] = ac.callsign.astype(str).str.strip().str.upper()
ac = ac.dropna(subset=["latitude","longitude"])
print(f"with position: {len(ac)}")

# MARKDOWN ********************

# ## Classify firefighting assets


# CELL ********************

# Identify firefighting aircraft
ac["is_firefighting"] = ac.callsign.str.startswith(FIRE_PREFIXES)
fire_ac = ac[ac.is_firefighting].copy()

print(f"firefighting aircraft airborne: {len(fire_ac)}")
if len(fire_ac):
    print(fire_ac[["callsign","latitude","longitude","baro_altitude",
                   "velocity","on_ground"]].to_string(index=False))
else:
    print("None airborne right now.")
    print("Securite Civile aircraft fly on demand, so outside an active")
    print("incident the fleet sits on the ground and broadcasts nothing.")

# Low-and-slow aircraft near a Securite Civile base are likely firefighting
# even when the callsign is a generic registration.
import math
def near_base(lat, lon, km=25.0):
    for name, (blat, blon) in SC_BASES.items():
        dx = (lon - blon) * 111.0 * math.cos(math.radians(lat))
        dy = (lat - blat) * 111.0
        if math.hypot(dx, dy) <= km:
            return name
    return None

ac["near_base"] = ac.apply(lambda r: near_base(r.latitude, r.longitude), axis=1)
candidates = ac[(ac.near_base.notna()) & (ac.baro_altitude.fillna(9e9) < 3000)
                & (~ac.is_firefighting)]
print(f"\nunlabelled low aircraft near SC bases: {len(candidates)}")
if len(candidates):
    print(candidates[["callsign","near_base","baro_altitude","velocity"]].head(10).to_string(index=False))

# MARKDOWN ********************

# ## Publish to Eventstream


# CELL ********************

# Publish everything, letting the Eventhouse decide what is firefighting.
# The full traffic picture is kept because a water bomber only becomes
# interesting relative to the airspace around it.
events = []
for _, r in ac.iterrows():
    events.append({
        "icao24":      str(r.icao24),
        "callsign":    r.callsign,
        "latitude":    float(r.latitude),
        "longitude":   float(r.longitude),
        "altitude_m":  None if pd.isna(r.baro_altitude) else float(r.baro_altitude),
        "velocity_ms": None if pd.isna(r.velocity)      else float(r.velocity),
        "heading_deg": None if pd.isna(r.true_track)    else float(r.true_track),
        "on_ground":   bool(r.on_ground),
        "origin_country": str(r.origin_country),
        "near_base":   r.near_base,
    })

publish(events, "aircraft positions", "aircraft")
