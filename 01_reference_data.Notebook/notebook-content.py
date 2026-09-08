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
# Builds the reference layer in `EH_Wildfire`: communes, fire stations, water
# bodies, critical infrastructure and GDACS alerts. Run it once before the
# streaming notebooks, and again only when the reference data should be rebuilt.
# It rewrites its target tables in place, so it has no schedule.
# 
# ### What to configure
# 
# Everything is in the **Configuration** cell, immediately below this one.
# 
# | Setting | Purpose | Notes |
# |---|---|---|
# | `WS` | Workspace id | See the table below |
# | `KQL_URI`, `KQL_DB` | Eventhouse to write into | See the table below |
# | `WEST, SOUTH, EAST, NORTH` | Area of interest | Currently metropolitan France and Corsica. Widening it increases Overpass load considerably. |
# | `UA` | User-Agent sent to OpenStreetMap | **Put a real contact address here.** The Overpass usage policy expects one, and requests may be throttled or refused without it. |
# | `OVERPASS_MIRRORS` | Endpoints tried in order | Mirrors are attempted in sequence on failure |
# 
# ### No API keys are required
# 
# This notebook uses only open endpoints: `geo.api.gouv.fr` for communes,
# OpenStreetMap Overpass for stations, water and infrastructure, and GDACS for
# alerts. None need registration. Overpass is a shared free service, so be
# considerate: run it on demand rather than on a schedule.
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
# Each write prints its target table and row count, and the final cell reports the
# row count per reference table. Empty tables mean a source or mirror failed.

# MARKDOWN ********************

# # 01 — Reference Data (France)
# 
# Loads the static reference layer from **real** public sources:
# 
# | Table | Source | What it is |
# |---|---|---|
# | `ref_communes` | geo.api.gouv.fr | ~35k official communes, population + centroid |
# | `ref_fire_stations` | OpenStreetMap / Overpass | ~6k real SDIS fire stations |
# | `ref_water_bodies` | OpenStreetMap / Overpass | Lakes and reservoirs, Canadair scooping points |
# | `ref_critical_infra` | OpenStreetMap / Overpass | Hospitals, schools, EHPAD, campsites |
# | `ref_ground_units` | derived from real stations | Vehicle rosters (status synthetic) |
# | `ref_gdacs_alerts` | GDACS | Live wildfire alert feed |
# 
# Run this **once** before the streaming notebooks. Re-run weekly to refresh.


# CELL ********************

%run 00_config

# CELL ********************

# Configuration

# France: metropolitan + Corsica
WEST, SOUTH, EAST, NORTH = -5.5, 41.0, 10.0, 51.5

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]

# CELL ********************

# Ingest helper: Spark DataFrame -> Eventhouse table
# The Kusto Spark connector is append-only, so "overwrite" is implemented as a
# KQL `.clear table ... data` management command followed by an append. That is
# the right semantic for reference tables, which are fully rebuilt each run.
import notebookutils, requests

def kql_mgmt(command):
    tok = notebookutils.credentials.getToken(KQL_URI)
    r = requests.post(f"{KQL_URI}/v1/rest/mgmt",
                      headers={"Authorization": f"Bearer {tok}",
                               "Content-Type": "application/json"},
                      json={"db": KQL_DB, "csl": command}, timeout=180)
    r.raise_for_status()
    return r.json()

def to_eventhouse(sdf, table, mode="append"):
    n = sdf.count()
    if mode == "overwrite":
        try:
            kql_mgmt(f".clear table {table} data")
            print(f"  cleared {table}")
        except Exception as e:
            print(f"  clear {table} skipped ({str(e)[:120]})")
    (sdf.write
        .format("com.microsoft.kusto.spark.synapse.datasource")
        .option("kustoCluster", KQL_URI)
        .option("kustoDatabase", KQL_DB)
        .option("kustoTable", table)
        .option("tableCreateOptions", "CreateIfNotExist")
        .option("accessToken", notebookutils.credentials.getToken(KQL_URI))
        .mode("append")
        .save())
    print(f"  -> {table}: {n} rows")

def overpass(query, label):
    """POST an Overpass QL query, walking mirrors on failure.

    Overpass rejects a raw body with HTTP 406; it must be form-encoded as
    data=<query> and carry a real User-Agent.
    """
    import requests, time
    last = None
    for attempt in range(3):
        for url in OVERPASS_MIRRORS:
            try:
                r = requests.post(url, data={"data": query}, headers=UA, timeout=300)
                r.raise_for_status()
                js = r.json()
                n = len(js.get("elements", []))
                if n:
                    print(f"{label}: {n} elements from {url}")
                    return js["elements"]
                last = f"{url} returned 0 elements"
            except Exception as e:
                last = f"{url} :: {e}"
        wait = 15 * (attempt + 1)
        print(f"{label}: all mirrors failed ({last}). Backing off {wait}s")
        time.sleep(wait)
    raise RuntimeError(f"{label}: Overpass unavailable. {last}")

def latlon(el):
    """Overpass nodes carry lat/lon; ways and relations carry center."""
    if "lat" in el:
        return el["lat"], el["lon"]
    c = el.get("center") or {}
    return c.get("lat"), c.get("lon")

# MARKDOWN ********************

# ## Communes


# CELL ********************

# Communes — geo.api.gouv.fr, the official French government open-data API.
# ~35k real communes with population and centroid.
import requests, pandas as pd

r = requests.get(
    "https://geo.api.gouv.fr/communes",
    params={"fields": "code,nom,codeDepartement,codeRegion,population,centre",
            "format": "json"},
    timeout=300)
r.raise_for_status()
raw = r.json()
print(f"communes returned: {len(raw)}")

rows = []
for c in raw:
    ctr = c.get("centre") or {}
    coords = ctr.get("coordinates") or []
    if len(coords) != 2:
        continue
    lon, lat = coords[0], coords[1]
    if not (WEST <= lon <= EAST and SOUTH <= lat <= NORTH):
        continue
    rows.append({
        "commune_id": c["code"],
        "commune":    c["nom"],
        "dept":       c.get("codeDepartement") or "",
        "region":     c.get("codeRegion") or "",
        "population": int(c.get("population") or 0),
        "latitude":   float(lat),
        "longitude":  float(lon),
    })

communes = pd.DataFrame(rows)
print(f"in France bbox: {len(communes)}   total population: {communes.population.sum():,}")
communes.head()

# CELL ********************

# s2_cell matches the Eventhouse silver layer (level 12) so joins line up.
from pyspark.sql import functions as F

sdf = spark.createDataFrame(communes)
sdf = sdf.withColumn("s2_cell", F.lit(""))   # filled by KQL geo_point_to_s2cell on read
to_eventhouse(sdf, "ref_communes", mode="overwrite")

# MARKDOWN ********************

# ## Fire stations


# CELL ********************

# Fire stations — OpenStreetMap via Overpass. These are real SDIS / SP stations.
q_stations = f"""
[out:json][timeout:280];
area["ISO3166-1"="FR"][admin_level=2]->.fr;
(
  node["amenity"="fire_station"](area.fr);
  way["amenity"="fire_station"](area.fr);
  relation["amenity"="fire_station"](area.fr);
);
out center;
"""
els = overpass(q_stations, "fire_stations")

rows = []
for el in els:
    lat, lon = latlon(el)
    if lat is None or not (WEST <= lon <= EAST and SOUTH <= lat <= NORTH):
        continue
    t = el.get("tags", {})
    rows.append({
        "station_id":   f"{el['type'][0].upper()}{el['id']}",
        "name":         t.get("name") or t.get("operator") or "Centre de secours",
        "operator":     t.get("operator") or "SDIS",
        "latitude":     float(lat),
        "longitude":    float(lon),
        "station_type": t.get("fire_station:type") or "CIS",
        "source":       "OpenStreetMap",
    })

stations = pd.DataFrame(rows).drop_duplicates("station_id")
print(f"fire stations in bbox: {len(stations)}")
to_eventhouse(spark.createDataFrame(stations), "ref_fire_stations", mode="overwrite")
stations.head()

# MARKDOWN ********************

# ## Water bodies (aerial refill)


# CELL ********************

# Water bodies — Canadair CL-415 scooping points.
# A CL-415 needs a run of roughly 1.3 km of open water, so tiny ponds are useless.
# Overpass gives no area directly, so filter on named lakes and reservoirs and
# mark the larger classes as scoopable.
q_water = f"""
[out:json][timeout:280];
area["ISO3166-1"="FR"][admin_level=2]->.fr;
(
  way["natural"="water"]["water"~"^(lake|reservoir|lagoon)$"]["name"](area.fr);
  relation["natural"="water"]["water"~"^(lake|reservoir|lagoon)$"]["name"](area.fr);
);
out center;
"""
els = overpass(q_water, "water_bodies")

rows = []
for el in els:
    lat, lon = latlon(el)
    if lat is None or not (WEST <= lon <= EAST and SOUTH <= lat <= NORTH):
        continue
    t = el.get("tags", {})
    wtype = t.get("water", "lake")
    rows.append({
        "water_id":   f"{el['type'][0].upper()}{el['id']}",
        "name":       t.get("name"),
        "water_type": wtype,
        "latitude":   float(lat),
        "longitude":  float(lon),
        # relations are multi-polygon and therefore large enough to scoop
        "scoopable":  bool(el["type"] == "relation" or wtype in ("lake", "reservoir")),
    })

water = pd.DataFrame(rows).drop_duplicates("water_id")
print(f"water bodies: {len(water)}   scoopable: {int(water.scoopable.sum())}")
to_eventhouse(spark.createDataFrame(water), "ref_water_bodies", mode="overwrite")
water.head()

# MARKDOWN ********************

# ## Critical infrastructure


# CELL ********************

# Critical infrastructure — schools, hospitals, care homes, campsites.
# These drive evacuation priority, so they are pulled for the fire-prone
# Mediterranean departments rather than all of France (query size).
q_infra = """
[out:json][timeout:280];
(
  area["ref:INSEE"="83"]; area["ref:INSEE"="13"]; area["ref:INSEE"="06"];
  area["ref:INSEE"="30"]; area["ref:INSEE"="34"]; area["ref:INSEE"="11"];
  area["ref:INSEE"="66"]; area["ref:INSEE"="2A"]; area["ref:INSEE"="2B"];
  area["ref:INSEE"="84"]; area["ref:INSEE"="04"]; area["ref:INSEE"="07"];
)->.med;
(
  node["amenity"~"^(hospital|school|kindergarten)$"](area.med);
  node["social_facility"="nursing_home"](area.med);
  node["tourism"="camp_site"](area.med);
  way["amenity"~"^(hospital|school)$"](area.med);
  way["tourism"="camp_site"](area.med);
);
out center;
"""
els = overpass(q_infra, "critical_infra")

TYPE_MAP = {"hospital":"Hopital","school":"Ecole","kindergarten":"Ecole maternelle",
            "nursing_home":"EHPAD","camp_site":"Camping"}

rows = []
for el in els:
    lat, lon = latlon(el)
    if lat is None or not (WEST <= lon <= EAST and SOUTH <= lat <= NORTH):
        continue
    t = el.get("tags", {})
    kind = t.get("amenity") or t.get("social_facility") or t.get("tourism") or "other"
    rows.append({
        "infra_id":   f"{el['type'][0].upper()}{el['id']}",
        "name":       t.get("name") or TYPE_MAP.get(kind, kind),
        "infra_type": TYPE_MAP.get(kind, kind),
        "latitude":   float(lat),
        "longitude":  float(lon),
        "commune":    t.get("addr:city") or "",
    })

infra = pd.DataFrame(rows).drop_duplicates("infra_id")
print(f"critical infra: {len(infra)}")
print(infra.infra_type.value_counts())
to_eventhouse(spark.createDataFrame(infra), "ref_critical_infra", mode="overwrite")

# MARKDOWN ********************

# ## Ground units


# CELL ********************

# Ground units — derived from the real fire stations pulled above.
# SDIS vehicle-level telemetry is not public, so unit rosters are generated per
# real station. Positions and identities are real; only status is synthetic.
import random
random.seed(2026)

MED_DEPTS_BOX = (2.0, 42.3, 9.6, 44.6)   # Mediterranean arc

def in_med(lat, lon):
    w, s, e, n = MED_DEPTS_BOX
    return w <= lon <= e and s <= lat <= n

med_stations = stations[stations.apply(lambda r: in_med(r.latitude, r.longitude), axis=1)]
print(f"stations in Mediterranean arc: {len(med_stations)}")

rows = []
for _, st in med_stations.head(400).iterrows():
    for i in range(random.randint(1, 3)):
        rows.append({
            "unit_id":    f"{st.station_id}-U{i+1}",
            "station_id": st.station_id,
            "unit_type":  random.choice(["CCF", "CCF", "CCFM", "FPT", "VLCG"]),
            "latitude":   float(st.latitude),
            "longitude":  float(st.longitude),
            "status":     random.choices(["available","engaged","maintenance"],
                                         weights=[75,20,5])[0],
            "is_simulated": True,
        })

ground = pd.DataFrame(rows)
ground["updated_at"] = pd.Timestamp.utcnow()
print(f"ground units: {len(ground)}   available: {(ground.status=='available').sum()}")
to_eventhouse(spark.createDataFrame(ground), "ref_ground_units", mode="overwrite")

# MARKDOWN ********************

# ## GDACS alerts


# CELL ********************

# GDACS — Global Disaster Alert and Coordination System, live wildfire alerts.
from datetime import datetime, timezone, timedelta

today = datetime.now(timezone.utc).date()
frm   = (today - timedelta(days=30)).isoformat()

r = requests.get(
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH",
    params={"eventlist":"WF", "fromdate":frm, "todate":today.isoformat()},
    headers=UA, timeout=180)
r.raise_for_status()
feats = r.json().get("features", [])
print(f"GDACS wildfire events (global, 30d): {len(feats)}")

rows = []
for f in feats:
    p = f.get("properties", {})
    g = f.get("geometry", {}) or {}
    coords = g.get("coordinates") or [None, None]
    lon, lat = coords[0], coords[1]
    if lat is None:
        continue
    rows.append({
        "event_id":    str(p.get("eventid")),
        "event_name":  p.get("name") or p.get("description") or "",
        "alert_level": p.get("alertlevel") or "",
        "country":     p.get("country") or "",
        "latitude":    float(lat),
        "longitude":   float(lon),
        "from_date":   pd.to_datetime(p.get("fromdate"), errors="coerce"),
        "to_date":     pd.to_datetime(p.get("todate"),   errors="coerce"),
        "glide":       p.get("glide") or "",
    })

gdacs = pd.DataFrame(rows)
gdacs["ingest_ts"] = pd.Timestamp.utcnow()
fr = gdacs[gdacs.country.str.contains("France", na=False)]
print(f"total {len(gdacs)}   France {len(fr)}")
if len(fr):
    print(fr[["event_name","alert_level","from_date"]].to_string())
to_eventhouse(spark.createDataFrame(gdacs), "ref_gdacs_alerts", mode="overwrite")

# MARKDOWN ********************

# ## Summary


# CELL ********************

# Verify what landed in the Eventhouse
from pyspark.sql import functions as F
print("Reference layer loaded:")
for t, df in [("ref_communes", communes), ("ref_fire_stations", stations),
              ("ref_water_bodies", water), ("ref_critical_infra", infra),
              ("ref_ground_units", ground), ("ref_gdacs_alerts", gdacs)]:
    print(f"  {t:<22} {len(df):>7} rows")
