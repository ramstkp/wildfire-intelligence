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
# Pulls active fire detections from NASA FIRMS and publishes them to
# `ES_Wildfire`, which routes them into `bronze_fire_raw`. Runs every 10 minutes
# to match the geostationary refresh cycle.
# 
# ### What to configure
# 
# Everything is in the **Configuration** cell, immediately below this one.
# 
# | Setting | Purpose | Notes |
# |---|---|---|
# | `FIRMS_KEY` | NASA FIRMS map key | **Required.** See below |
# | `WS`, `ES_ID`, `ES_SRC` | Where to publish | See the table below |
# | `WEST, SOUTH, EAST, NORTH` | Area of interest | FIRMS expects west, south, east, north |
# | `TIERS` | Satellite products queried | Geostationary gives cadence, polar gives resolution |
# 
# ### Getting a NASA FIRMS map key
# 
# 1. Go to <https://firms.modaps.eosdis.nasa.gov/api/area/>
# 2. Select **Get MAP_KEY** and submit your email address.
# 3. The key arrives by email, usually within a few minutes.
# 4. Paste it as `FIRMS_KEY` in the configuration cell.
# 
# The key is free. Its quota is roughly **5,000 transactions per 10 minutes**; one
# run of this notebook uses one transaction per entry in `TIERS`. If a request
# returns `Invalid MAP_KEY` the key is wrong or has been revoked; if it returns
# an empty body the quota is exhausted, so wait for the window to reset.
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
# Cell output reports rows fetched per satellite product, how many were new after
# deduplication, and how many were published. The final cell queries
# `silver_fire_detections` in the Eventhouse. Allow **3 to 6 minutes** for
# Eventstream batching before rows appear.
# 
# FIRMS serves a rolling window, so most of what each run receives has already
# been published. That is expected: the notebook filters against the Eventhouse
# and sends only what is new.


# MARKDOWN ********************

# # 02 - Satellite Fire Detections
# 
# Pulls **real** active-fire observations from NASA FIRMS and streams them to
# `ES_Wildfire`, which routes them into `bronze_fire_raw`.
# 
# ## Why two satellite tiers
# 
# | Tier | Satellites | Refresh | Resolution | What it gives us |
# |---|---|---|---|---|
# | Geostationary | Met12 (MTG-I1), Met10, Met9 | **10-15 min** | ~2 km | Fire *growth* over time |
# | Polar orbit | VIIRS SNPP / NOAA-20 / NOAA-21 | 2-4 passes/day | **375 m** | Fire *shape* and precision |
# | Polar orbit | MODIS Aqua + Terra | 2 passes/day | 1 km | Long historical baseline |
# 
# A polar orbiter gives two snapshots a day, which can tell you a fire exists
# but never that it is accelerating. Met12 revisiting every 10 minutes is what
# turns this from a map into an early-warning system.
# 
# **Schedule this notebook every 10 minutes** to match the Met12 cadence.


# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fail fast rather than calling FIRMS with an empty key.
if not FIRMS_KEY:
    raise RuntimeError(
        "FIRMS_KEY is empty. Set it in the credentials cell of 00_config. "
        "The key is free from "
        "https://firms.modaps.eosdis.nasa.gov/api/area/")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration

# France: metropolitan + Corsica. FIRMS wants west,south,east,north.
WEST, SOUTH, EAST, NORTH = -5.5, 41.0, 10.0, 51.5
BBOX = f"{WEST},{SOUTH},{EAST},{NORTH}"

# Satellite tiers.
#
# GEOSTATIONARY : parked over the equator, staring at the same hemisphere
# continuously. Over Europe the FIRMS "GOES_NRT" product actually serves
# Meteosat (Met12/MTG-I1, Met10, Met9). Met12 refreshes every 10 minutes.
# Coarse (~2 km) but it is the only thing that can show a fire GROWING.
#
# POLAR ORBIT : circles pole to pole ~14x/day, imaging a swath each pass.
# Any point on Earth gets 2-4 passes/day, but at 375 m VIIRS resolves the
# actual fire perimeter. NRT products lag ~1 day, so dayRange=2.
TIERS = [
    # (product,           day_range, sensor_class,   note)
    ("GOES_NRT",          1, "geostationary", "Meteosat, 10-15 min refresh"),
    ("VIIRS_SNPP_NRT",    2, "polar",         "375 m, Suomi-NPP"),
    ("VIIRS_NOAA20_NRT",  2, "polar",         "375 m, NOAA-20"),
    ("VIIRS_NOAA21_NRT",  2, "polar",         "375 m, NOAA-21"),
    ("MODIS_NRT",         1, "polar",         "1 km, Aqua + Terra"),
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Eventstream publisher
#
# Two things to know about this path:
#
# 1. The `azure-eventhub` SDK is not in the Fabric Spark runtime, so events are
#    sent with the Event Hubs REST API instead. That avoids a %pip install on
#    every scheduled run.
# 2. The Eventstream maps incoming JSON to the destination table BY COLUMN NAME.
#    `bronze_fire_raw` is (event:dynamic, ingest_ts:datetime), so the payload
#    must be shaped {"event": {...}, "ingest_ts": ...}. A flat payload silently
#    lands rows with an empty `event`, and the update policy then drops them.
import requests, json, time, base64, hashlib, hmac, urllib.parse, notebookutils

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
                k, _, v = part.partition("=")
                p[k] = v
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
    """Wrap each event as {event_type, event, ingest_ts} and POST in batches."""
    if not events:
        print(f"{label}: nothing to publish")
        return 0
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch all sensor tiers


# CELL ********************

# Pull every sensor tier from FIRMS
import pandas as pd, io, requests
from datetime import datetime, timezone

def firms(product, day_range):
    url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
           f"{FIRMS_KEY}/{product}/{BBOX}/{day_range}")
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    body = r.text.strip()
    if not body or body.lower().startswith("invalid"):
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(body))
    return df

frames = []
for product, days, sensor_class, note in TIERS:
    try:
        df = firms(product, days)
        if df.empty:
            print(f"{product:<18} 0 rows      ({note})")
            continue
        df["product"]      = product
        df["sensor_class"] = sensor_class
        # MODIS names its channels differently to VIIRS/Meteosat.
        if "brightness" not in df.columns and "bright_ti4" in df.columns:
            df["brightness"] = df["bright_ti4"]
        frames.append(df)
        print(f"{product:<18} {len(df):>5} rows  ({note})")
    except Exception as e:
        print(f"{product:<18} FAILED  {e}")

fires = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
print(f"\nTOTAL raw detections: {len(fires)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Geostationary cadence check


# CELL ********************

# What the geostationary feed buys us: repeated frames of the same fire.
geo = fires[fires.sensor_class == "geostationary"].copy()
if len(geo):
    geo["slot"] = geo.acq_date.astype(str) + " " + geo.acq_time.astype(str)
    per_sat = geo.groupby("satellite").agg(
        detections=("frp", "size"),
        frames=("slot", "nunique"),
        max_frp=("frp", "max"))
    print("Geostationary refresh (this is what makes growth measurable):")
    print(per_sat.to_string())
else:
    print("No geostationary rows this run.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Normalise


# CELL ********************

# Normalise into the shape ParseFireDetections() expects in the Eventhouse
from datetime import datetime, timezone

if fires.empty:
    raise RuntimeError("FIRMS returned no detections for France. Check the map key quota.")

def pad_time(v):
    """FIRMS drops leading zeros: 8 means 00:08, 319 means 03:19."""
    return str(int(v)).zfill(4)

fires["acq_time"] = fires["acq_time"].apply(pad_time)
fires["frp"]      = pd.to_numeric(fires["frp"], errors="coerce").fillna(0.0)
fires["brightness"] = pd.to_numeric(fires.get("brightness"), errors="coerce")

events = []
for _, r in fires.iterrows():
    events.append({
        "latitude":    float(r["latitude"]),
        "longitude":   float(r["longitude"]),
        "frp":         float(r["frp"]),
        "brightness":  None if pd.isna(r.get("brightness")) else float(r["brightness"]),
        "confidence":  str(r.get("confidence", "")),
        "satellite":   str(r.get("satellite", "")),
        "instrument":  str(r.get("instrument", "") or ""),
        "acq_date":    str(r["acq_date"]),
        "acq_time":    r["acq_time"],
        "daynight":    str(r.get("daynight", "")),
        "product":     r["product"],
        "sensor_class": r["sensor_class"],
    })

print(f"events ready: {len(events)}")
print(f"  geostationary: {sum(1 for e in events if e['sensor_class']=='geostationary')}")
print(f"  polar        : {sum(1 for e in events if e['sensor_class']=='polar')}")
print(f"  max FRP      : {max(e['frp'] for e in events):.1f} MW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drop detections already ingested
# 
# FIRMS returns a rolling window, so most of what each run receives has already
# been published by an earlier run. Left unfiltered, the same detection lands
# repeatedly and every `sum(frp)` downstream is multiplied by the number of times
# it was re-delivered.
# 
# The Eventhouse is the state store: ask it which detections it already holds,
# and send only the remainder. No local checkpoint to drift out of sync.

# CELL ********************

# Filter out detections the Eventhouse already holds
#
# The comparison key mirrors detection_id in ParseFireDetections, but both
# sides are formatted here in Python rather than reproducing Kusto's
# tostring(real) behaviour, which differs on trailing zeros and whole numbers.
import notebookutils

def _key(lat, lon, acq_compact, sat):
    return (f"{float(lat):.4f}", f"{float(lon):.4f}", str(acq_compact), str(sat))

def already_ingested(window="72h"):
    """Keys currently in the Eventhouse. Empty set on any failure, which makes
    this a pure optimisation: worst case we republish and gold_fire_dedup
    absorbs it."""
    q = f"""
    gold_fire_dedup
    | where acq_datetime > ago({window})
    | project latitude, longitude,
              acq = format_datetime(acq_datetime, 'yyyyMMddHHmm'),
              satellite
    """
    try:
        tok = notebookutils.credentials.getToken(KQL_URI)
        df = (spark.read
              .format("com.microsoft.kusto.spark.synapse.datasource")
              .option("kustoCluster", KQL_URI)
              .option("kustoDatabase", KQL_DB)
              .option("kustoQuery", q)
              .option("accessToken", tok)
              .load())
        return {_key(r["latitude"], r["longitude"], r["acq"], r["satellite"])
                for r in df.collect()}
    except Exception as e:
        print(f"could not read existing detections ({e})")
        print("publishing everything; gold_fire_dedup will still dedupe")
        return set()

seen = already_ingested()
print(f"already in Eventhouse: {len(seen)}")

fresh = []
for e in events:
    k = _key(e["latitude"], e["longitude"],
             str(e["acq_date"]).replace("-", "") + e["acq_time"],
             e["satellite"])
    if k not in seen:
        fresh.append(e)

dupes = len(events) - len(fresh)
pct = (100.0 * dupes / len(events)) if events else 0.0
print(f"FIRMS returned : {len(events)}")
print(f"already seen   : {dupes}  ({pct:.0f}% of the window)")
print(f"new to publish : {len(fresh)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Publish to Eventstream


# CELL ********************

# Publish only the new detections. The Filter operator routes event_type='fire'
# into bronze_fire_raw, where the update policy parses it into
# silver_fire_detections and gold_fire_dedup guarantees one row per detection.
publish(fresh, "new fire detections", "fire")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Verify


# CELL ********************

# Confirm the data landed
import time, notebookutils
time.sleep(30)   # allow the streaming pipeline to flush

from pyspark.sql import functions as F
tok = notebookutils.credentials.getToken(KQL_URI)
q = """
silver_fire_detections
| summarize detections=count(), max_frp=max(frp), latest=max(acq_datetime) by sensor_class
"""
df = (spark.read
      .format("com.microsoft.kusto.spark.synapse.datasource")
      .option("kustoCluster", KQL_URI)
      .option("kustoDatabase", KQL_DB)
      .option("kustoQuery", q)
      .option("accessToken", tok)
      .load())
df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
