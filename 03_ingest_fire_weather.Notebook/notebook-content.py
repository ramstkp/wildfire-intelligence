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
# Pulls surface and upper-air weather from Open-Meteo, computes the Canadian Fire
# Weather Index, and publishes to `ES_Wildfire`, which routes into
# `bronze_weather_raw`. Runs every 15 minutes.
# 
# ### What to configure
# 
# Everything is in the **Configuration** cell, immediately below this one.
# 
# | Setting | Purpose | Notes |
# |---|---|---|
# | `WS`, `ES_ID`, `ES_SRC` | Where to publish | See the table below |
# | `KQL_URI`, `KQL_DB` | Eventhouse read for active fire locations | The grid follows the fires |
# | `FALLBACK_GRID` | Points used when nothing is burning | Mediterranean arc by default |
# | `MAX_POINTS` | Cap on grid points per run | Each point is roughly three Open-Meteo calls |
# 
# ### No API key is required
# 
# Open-Meteo is free for non-commercial use and needs no registration. It is rate
# limited by IP, so keep `MAX_POINTS` modest; the default of 24 is comfortable at
# a 15-minute cadence. For heavier or commercial use, Open-Meteo offers a paid
# tier with an API key, which would be added to the request parameters.
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
# The notebook prints how many grid points were built, how many weather records
# were retrieved, the fire-danger distribution, and how many events were
# published. Verify with `silver_weather` in the Eventhouse.
# 
# Note that this notebook has no verification cell of its own: if Open-Meteo
# returns nothing it prints "nothing to publish" and still finishes successfully.
# Always confirm against the Eventhouse rather than trusting the run status.

# MARKDOWN ********************

# # 03 — Fire Weather and Wind
# 
# Streams real meteorology into `bronze_weather_raw`. Weather is what turns a
# hotspot into a forecast.
# 
# ## What is collected and why
# 
# | Signal | Why it matters |
# |---|---|
# | Wind speed, direction, gusts (10 m) | Drives where the **fire front** advances |
# | Wind at 925 / 850 / 700 hPa | Drives where the **smoke plume** travels, often a different direction |
# | 48 h wind forecast | Lets the gold layer project spread forward |
# | Humidity, dew point, VPD | Dry air pulls moisture out of fuel |
# | Soil moisture | Antecedent drought state |
# | Elevation | Fire climbs slope roughly twice as fast per 10 degrees |
# | PM2.5, PM10, aerosol depth | Observed smoke impact on communities |
# 
# ## Fire Weather Index
# 
# EFFIS publishes the Canadian FWI for Europe, but only as display-only WMS
# rasters (`GetFeatureInfo` is disabled), and Open-Meteo has no FWI parameter.
# So the full FFMC / DMC / DC / ISI / BUI / FWI chain is computed here from the
# real observations using the Van Wagner equations.
# 
# **Schedule every 15 minutes.**


# CELL ********************

%run 00_config

# CELL ********************

# Configuration

# Sampling weather across all of France at fine resolution would be thousands of
# API calls. Instead the grid is placed where fires actually are: around live
# detections, falling back to the Mediterranean arc when nothing is burning.
FALLBACK_GRID = [
    (43.15, 6.34),  (43.12, 5.93),  (43.53, 5.45),  (43.54, 6.47),
    (43.66, 6.92),  (43.84, 4.36),  (43.61, 3.88),  (42.70, 2.90),
    (42.70, 9.45),  (41.93, 8.74),  (44.13, 5.05),  (43.43, 6.74),
    (44.56, 4.75),  (43.29, 3.20),  (42.35, 2.75),  (44.35, 6.65),
]
MAX_POINTS = 24

# CELL ********************

# Eventstream publisher
#
# The azure-eventhub SDK is not in the Fabric runtime, so this uses the Event
# Hubs REST API. The Eventstream maps JSON to the destination table by column
# name, and bronze_weather_raw is (event:dynamic, ingest_ts:datetime), so the
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

def publish(events, label, event_type, batch_size=100):
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

# ## Build the grid around live fires


# CELL ********************

# Put the weather grid where the fires are
import notebookutils

def active_fire_points():
    tok = notebookutils.credentials.getToken(KQL_URI)
    q = """
    gold_fire_zones
    | where frame > ago(6h)
    | summarize frp = sum(frp_total), lat = avg(fire_lat), lon = avg(fire_lon) by s2_cell
    | top 20 by frp desc
    | project lat, lon
    """
    try:
        df = (spark.read
              .format("com.microsoft.kusto.spark.synapse.datasource")
              .option("kustoCluster", KQL_URI)
              .option("kustoDatabase", KQL_DB)
              .option("kustoQuery", q)
              .option("accessToken", tok)
              .load())
        return [(float(r["lat"]), float(r["lon"])) for r in df.collect()]
    except Exception as e:
        print(f"could not read active zones ({e})")
        return []

pts = active_fire_points()
print(f"active fire points: {len(pts)}")

# Round to ~0.1 deg so nearby detections collapse onto one grid cell,
# matching the grid_id the Eventhouse parse function builds.
seen, grid = set(), []
for lat, lon in pts + FALLBACK_GRID:
    key = (round(lat, 1), round(lon, 1))
    if key in seen:
        continue
    seen.add(key); grid.append(key)
    if len(grid) >= MAX_POINTS:
        break

print(f"weather grid points: {len(grid)}")
print(grid[:8])

# MARKDOWN ********************

# ## Fetch weather, upper-air wind and air quality


# CELL ********************

# Open-Meteo: surface fire weather + upper-air wind + air quality
import requests, pandas as pd
from datetime import datetime, timezone

SURFACE = ",".join([
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "soil_moisture_0_to_1cm", "vapour_pressure_deficit",
    "precipitation", "surface_pressure", "cape",
])

# Upper-air wind matters for a different reason to surface wind.
# Surface wind drives where the FIRE goes; 850 hPa (~1500 m) drives where the
# SMOKE goes. They frequently point in different directions, which is why a
# commune can be unaffected by flames yet blanketed in smoke.
UPPER = ",".join([
    "wind_speed_925hPa", "wind_direction_925hPa",
    "wind_speed_850hPa", "wind_direction_850hPa",
    "wind_speed_700hPa", "wind_direction_700hPa",
])

FORECAST = "wind_speed_10m,wind_direction_10m,wind_gusts_10m,temperature_2m,relative_humidity_2m,precipitation"

def fetch_point(lat, lon):
    w = requests.get("https://api.open-meteo.com/v1/forecast", params={
        "latitude": lat, "longitude": lon,
        "current": SURFACE, "hourly": UPPER + "," + FORECAST,
        "forecast_days": 2, "timezone": "UTC"}, timeout=90)
    w.raise_for_status()
    wj = w.json()

    aq = {}
    try:
        a = requests.get("https://air-quality-api.open-meteo.com/v1/air-quality", params={
            "latitude": lat, "longitude": lon,
            "current": "pm2_5,pm10,carbon_monoxide,aerosol_optical_depth,dust",
            "timezone": "UTC"}, timeout=60)
        a.raise_for_status()
        aq = a.json().get("current", {})
    except Exception:
        pass

    el = None
    try:
        e = requests.get("https://api.open-meteo.com/v1/elevation",
                         params={"latitude": lat, "longitude": lon}, timeout=45)
        e.raise_for_status()
        el = (e.json().get("elevation") or [None])[0]
    except Exception:
        pass

    return wj, aq, el

records = []
for lat, lon in grid:
    try:
        wj, aq, el = fetch_point(lat, lon)
        cur = wj.get("current", {})
        h   = wj.get("hourly", {})
        # index 0 of hourly is the current hour
        records.append({
            "latitude": lat, "longitude": lon, "elevation_m": el,
            "temperature_c":      cur.get("temperature_2m"),
            "humidity_pct":       cur.get("relative_humidity_2m"),
            "dew_point_c":        cur.get("dew_point_2m"),
            "wind_speed_kmh":     cur.get("wind_speed_10m"),
            "wind_direction_deg": cur.get("wind_direction_10m"),
            "wind_gust_kmh":      cur.get("wind_gusts_10m"),
            "soil_moisture":      cur.get("soil_moisture_0_to_1cm"),
            "vpd_kpa":            cur.get("vapour_pressure_deficit"),
            "precipitation_mm":   cur.get("precipitation"),
            "pressure_hpa":       cur.get("surface_pressure"),
            "cape":               cur.get("cape"),
            "wind_925_speed":     (h.get("wind_speed_925hPa")     or [None])[0],
            "wind_925_dir":       (h.get("wind_direction_925hPa") or [None])[0],
            "wind_850_speed":     (h.get("wind_speed_850hPa")     or [None])[0],
            "wind_850_dir":       (h.get("wind_direction_850hPa") or [None])[0],
            "wind_700_speed":     (h.get("wind_speed_700hPa")     or [None])[0],
            "wind_700_dir":       (h.get("wind_direction_700hPa") or [None])[0],
            "pm2_5":              aq.get("pm2_5"),
            "pm10":               aq.get("pm10"),
            "aod":                aq.get("aerosol_optical_depth"),
            "observed_at":        cur.get("time"),
            # next 12 h of wind, so the gold layer can project spread forward
            "fc_wind_dir":        (h.get("wind_direction_10m") or [])[:12],
            "fc_wind_speed":      (h.get("wind_speed_10m")     or [])[:12],
            "fc_wind_gust":       (h.get("wind_gusts_10m")     or [])[:12],
        })
    except Exception as e:
        print(f"  {lat},{lon} failed: {e}")

wx = pd.DataFrame(records)
print(f"weather records: {len(wx)}")
wx[["latitude","longitude","temperature_c","humidity_pct",
    "wind_speed_kmh","wind_direction_deg","vpd_kpa"]].head(10)

# MARKDOWN ********************

# ## Plume shear: where smoke and fire diverge


# CELL ********************

# Wind shear: surface versus 850 hPa.
# Where these diverge, the smoke plume travels somewhere the fire front does not.
import math

def bearing_delta(a, b):
    if a is None or b is None: return None
    return abs((a - b + 540) % 360 - 180)

wx["plume_shear_deg"] = wx.apply(
    lambda r: bearing_delta(r.wind_direction_deg, r.wind_850_dir), axis=1)

sheared = wx[wx.plume_shear_deg > 45].copy()
print(f"grid points where smoke and fire diverge >45 deg: {len(sheared)} of {len(wx)}")
if len(sheared):
    print(sheared[["latitude","longitude","wind_direction_deg",
                   "wind_850_dir","plume_shear_deg"]].head(10).to_string(index=False))

# MARKDOWN ********************

# ## Compute the Fire Weather Index


# CELL ********************

# Canadian Forest Fire Weather Index.
#
# EFFIS publishes FWI as WMS raster layers but GetFeatureInfo is disabled on
# them, and Open-Meteo has no FWI parameter. So it is computed here from the
# real observations using the Van Wagner / Canadian Forest Service equations.
#
# FFMC = fine fuel moisture   -> ignition ease (litter, needles)
# DMC  = duff moisture        -> medium-depth organic layers
# DC   = drought code         -> deep, slow-drying organic matter
# ISI  = initial spread index -> FFMC combined with wind
# BUI  = build-up index       -> DMC combined with DC, total available fuel
# FWI  = final index          -> ISI combined with BUI
import math

def ffmc_calc(temp, rh, wind, rain, ffmc_prev=85.0):
    mo = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)
    if rain > 0.5:
        rf = rain - 0.5
        mo += (42.5 * rf * math.exp(-100.0 / (251.0 - mo)) * (1.0 - math.exp(-6.93 / rf)))
        if mo > 150.0:
            mo += 0.0015 * (mo - 150.0) ** 2 * math.sqrt(rf)
        mo = min(mo, 250.0)
    ed = (0.942 * rh ** 0.679 + 11.0 * math.exp((rh - 100.0) / 10.0)
          + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh)))
    if mo > ed:
        ko = (0.424 * (1.0 - (rh / 100.0) ** 1.7)
              + 0.0694 * math.sqrt(wind) * (1.0 - (rh / 100.0) ** 8))
        kd = ko * 0.581 * math.exp(0.0365 * temp)
        m = ed + (mo - ed) * 10.0 ** (-kd)
    else:
        ew = (0.618 * rh ** 0.753 + 10.0 * math.exp((rh - 100.0) / 10.0)
              + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh)))
        if mo < ew:
            kl = (0.424 * (1.0 - ((100.0 - rh) / 100.0) ** 1.7)
                  + 0.0694 * math.sqrt(wind) * (1.0 - ((100.0 - rh) / 100.0) ** 8))
            kw = kl * 0.581 * math.exp(0.0365 * temp)
            m = ew - (ew - mo) * 10.0 ** (-kw)
        else:
            m = mo
    return max(0.0, min(101.0, 59.5 * (250.0 - m) / (147.2 + m)))

def dmc_calc(temp, rh, rain, month, dmc_prev=6.0):
    DAY_LENGTH = [6.5,7.5,9.0,12.8,13.9,13.9,12.4,10.9,9.4,8.0,7.0,6.0]
    p = dmc_prev
    if rain > 1.5:
        re = 0.92 * rain - 1.27
        mo = 20.0 + math.exp(5.6348 - p / 43.43)
        if   p <= 33.0: b = 100.0 / (0.5 + 0.3 * p)
        elif p <= 65.0: b = 14.0 - 1.3 * math.log(p)
        else:           b = 6.2 * math.log(p) - 17.2
        mr = mo + 1000.0 * re / (48.77 + b * re)
        p = max(0.0, 244.72 - 43.43 * math.log(mr - 20.0))
    t = max(temp, -1.1)
    k = 1.894 * (t + 1.1) * (100.0 - rh) * DAY_LENGTH[month - 1] * 1e-6
    return max(0.0, p + 100.0 * k)

def dc_calc(temp, rain, month, dc_prev=15.0):
    LF = [-1.6,-1.6,-1.6,0.9,3.8,5.8,6.4,5.0,2.4,0.4,-1.6,-1.6]
    d = dc_prev
    if rain > 2.8:
        rd = 0.83 * rain - 1.27
        Qo = 800.0 * math.exp(-d / 400.0)
        Qr = Qo + 3.937 * rd
        d = max(0.0, 400.0 * math.log(800.0 / Qr))
    t = max(temp, -2.8)
    v = 0.36 * (t + 2.8) + LF[month - 1]
    return max(0.0, d + 0.5 * max(v, 0.0))

def isi_calc(ffmc, wind):
    m = 147.2 * (101.0 - ffmc) / (59.5 + ffmc)
    ff = 19.115 * math.exp(-0.1386 * m) * (1.0 + m ** 5.31 / 4.93e7)
    return ff * math.exp(0.05039 * wind)

def bui_calc(dmc, dc):
    if dmc == 0 and dc == 0: return 0.0
    if dmc <= 0.4 * dc:
        return 0.8 * dmc * dc / (dmc + 0.4 * dc)
    return dmc - (1.0 - 0.8 * dc / (dmc + 0.4 * dc)) * (0.92 + (0.0114 * dmc) ** 1.7)

def fwi_calc(isi, bui):
    fD = (0.626 * bui ** 0.809 + 2.0) if bui <= 80.0 else \
         1000.0 / (25.0 + 108.64 * math.exp(-0.023 * bui))
    B = 0.1 * isi * fD
    if B <= 1.0: return B
    return math.exp(2.72 * (0.434 * math.log(B)) ** 0.647)

month = pd.Timestamp.utcnow().month
def row_fwi(r):
    try:
        t  = float(r.temperature_c); rh = float(r.humidity_pct)
        w  = float(r.wind_speed_kmh); rn = float(r.precipitation_mm or 0.0)
        # Soil moisture stands in for antecedent drying, so the codes start
        # from a realistic state rather than a fixed default.
        sm = float(r.soil_moisture or 0.15)
        dryness = max(0.0, min(1.0, (0.30 - sm) / 0.30))
        ffmc = ffmc_calc(t, rh, w, rn, 70.0 + 25.0 * dryness)
        dmc  = dmc_calc(t, rh, rn, month, 5.0 + 40.0 * dryness)
        dc   = dc_calc(t, rn, month, 50.0 + 400.0 * dryness)
        isi  = isi_calc(ffmc, w)
        bui  = bui_calc(dmc, dc)
        return pd.Series({"ffmc":round(ffmc,1), "dmc":round(dmc,1), "dc":round(dc,1),
                          "isi":round(isi,1), "bui":round(bui,1),
                          "fwi":round(fwi_calc(isi, bui),1)})
    except Exception:
        return pd.Series({"ffmc":None,"dmc":None,"dc":None,"isi":None,"bui":None,"fwi":None})

wx = wx.join(wx.apply(row_fwi, axis=1))

def danger(f):
    if f is None: return "UNKNOWN"
    if f < 5.2:   return "LOW"
    if f < 11.2:  return "MODERATE"
    if f < 21.3:  return "HIGH"
    if f < 38.0:  return "VERY HIGH"
    return "EXTREME"

wx["fire_danger"] = wx.fwi.apply(danger)
print(wx.fire_danger.value_counts().to_string())
wx[["latitude","longitude","temperature_c","humidity_pct","wind_speed_kmh",
    "ffmc","isi","bui","fwi","fire_danger"]].sort_values("fwi", ascending=False).head(12)

# MARKDOWN ********************

# ## Publish to Eventstream


# CELL ********************

# Publish. event_type='weather' routes to bronze_weather_raw.
events = []
for _, r in wx.iterrows():
    d = r.to_dict()
    clean = {}
    for k, v in d.items():
        if isinstance(v, list):
            clean[k] = v
        elif pd.isna(v):
            clean[k] = None
        else:
            clean[k] = v
    clean["observed_at"] = clean.get("observed_at") or datetime.now(timezone.utc).isoformat()
    events.append(clean)

publish(events, "weather observations", "weather")
