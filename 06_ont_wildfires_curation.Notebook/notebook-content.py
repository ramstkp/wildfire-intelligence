# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5ce1dc25-d2d8-4343-8adf-f9af1858e36f",
# META       "default_lakehouse_name": "LH_WildFires",
# META       "default_lakehouse_workspace_id": "6dfa57e6-d36c-4a15-a745-0a1a0bd3e9b1"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 06 - Ontology base over the mirrored Eventhouse
# 
# Builds the ontology entity, bridge and time-series tables in
# `LH_WildFires` under schema **`ont_wildfires`**, reading the OneLake shortcuts
# of the mirrored Eventhouse tables under schema **`rt_fires`**.
# 
# ## How this differs from notebook 05
# 
# | | 05_gold_curation | 06 (this notebook) |
# |---|---|---|
# | Source | KQL queries against the Eventhouse | Delta shortcuts in `rt_fires` |
# | Compute | Kusto engine, then collect | Spark, straight off OneLake |
# | Reprocessing | Full rebuild every run | Incremental, watermark driven |
# | Target | `LH_Wildfire` default schema | `LH_WildFires.ont_wildfires` |
# 
# Mirroring puts the Eventhouse tables in OneLake as Delta at a 5 minute target
# latency, so the ontology no longer needs a Kusto round trip to be fed.
# 
# ## Processing model
# 
# Not everything can be incremental in the same way, so each family is handled
# according to how it actually behaves:
# 
# | Family | Strategy | Why |
# |---|---|---|
# | `ent_commune`, `ent_fire_station`, `ent_water_body`, `ent_critical_infra` | merge on key | Reference data, rarely changes |
# | `ent_aircraft`, `ent_weather_grid` | merge latest-per-key from new rows only | Cumulative current state, grows by key |
# | `ent_fire_zone` | recompute over lookback, overwrite | Bounded active set, zones must age out |
# | `bridge_*` | recompute for active zones, overwrite | Derived entirely from the active set |
# | `ts_*` | merge new rows on (key, timestamp) | Unbounded growth, must not rescan history |
# 
# The gold materialized views are **not** mirrored, only base tables are, so the
# `gold_fire_zones` / `gold_weather_latest` / `gold_aircraft_latest` logic is
# reproduced here in Spark against the silver shortcuts.
# 
# ## One trap worth knowing
# 
# In a schema-enabled lakehouse the Spark session's current database is an
# internal scratch namespace, **not** the lakehouse. A write to an unqualified
# `ont_wildfires.ent_commune` is accepted, lands nowhere useful, and the notebook
# still reports success. Every table reference here is therefore fully qualified
# as `LH_WildFires.<schema>.<table>`.


# CELL ********************

%run 00_config

# CELL ********************

# Configuration
WS_ID   = WS

SRC_SCHEMA = "rt_fires"        # shortcuts to the mirrored Eventhouse tables
DST_SCHEMA = "ont_wildfires"   # ontology base built by this notebook

# A schema-enabled lakehouse puts the Spark session in a scratch database, not
# in the lakehouse, so a bare "ont_wildfires.x" resolves somewhere harmless and
# the write silently goes nowhere. Every name must be lakehouse-qualified.
SRC_FQ = f"{LH_NAME}.{SRC_SCHEMA}"
DST_FQ = f"{LH_NAME}.{DST_SCHEMA}"

LOOKBACK_H = 24      # hours of detections that define the active fire set
MIN_FRP    = 3.0     # MW, below this is usually industrial heat or noise
THREAT_KM  = 30.0    # commune threat radius
STATION_KM = 40.0    # station coverage radius
WATER_KM   = 60.0    # scooping range from a fire
FRAME_MIN  = 10      # detection frame size, matches gold_fire_zones

# First run has no watermark. This bounds how much history is pulled in that
# case so the initial build cannot run away.
COLD_START_H = 48

import datetime as dt
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

RUN_TS = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
print(f"run start (UTC): {RUN_TS}")
print(f"source: {SRC_FQ}")
print(f"target: {DST_FQ}")

def src(table):
    """Read a shortcut in the rt_fires schema."""
    return spark.table(f"{SRC_FQ}.{table}")

def dst_name(table):
    return f"{DST_FQ}.{table}"

# MARKDOWN ********************

# ## Schema and watermark control table
# 
# `ctl_watermark` records, per source table, the highest `ingest_ts` that has
# already been folded into the ontology. Everything incremental keys off it.

# CELL ********************

# Create the target schema and the watermark control table
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DST_FQ}")

WM_TABLE = dst_name("ctl_watermark")

WM_SCHEMA = T.StructType([
    T.StructField("source_table", T.StringType(), False),
    T.StructField("last_ts",      T.TimestampType(), True),
    T.StructField("rows_seen",    T.LongType(), True),
    T.StructField("updated_at",   T.TimestampType(), True),
])

if not spark.catalog.tableExists(WM_TABLE):
    (spark.createDataFrame([], WM_SCHEMA)
          .write.format("delta").mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(WM_TABLE))
    print(f"created {WM_TABLE}")
else:
    print(f"{WM_TABLE} exists")

def get_watermark(table):
    """Last processed ingest_ts for a source table, or the cold-start floor."""
    row = (spark.table(WM_TABLE)
                .where(F.col("source_table") == table)
                .select("last_ts").head())
    if row and row["last_ts"] is not None:
        return row["last_ts"]
    return RUN_TS - dt.timedelta(hours=COLD_START_H)

def set_watermark(table, last_ts, rows_seen):
    """Upsert the watermark. Never moves backwards."""
    if last_ts is None:
        return
    upd = spark.createDataFrame(
        [(table, last_ts, int(rows_seen), RUN_TS)], WM_SCHEMA)
    (DeltaTable.forName(spark, WM_TABLE).alias("t")
        .merge(upd.alias("s"), "t.source_table = s.source_table")
        .whenMatchedUpdate(
            condition="s.last_ts > t.last_ts",
            set={"last_ts": "s.last_ts", "rows_seen": "s.rows_seen",
                 "updated_at": "s.updated_at"})
        .whenNotMatchedInsertAll()
        .execute())

for t in ["silver_fire_detections", "silver_weather", "silver_aircraft"]:
    print(f"  watermark {t:<24} {get_watermark(t)}")

# MARKDOWN ********************

# ## Pick up the deltas
# 
# Only rows newer than the watermark are read. The watermark is captured *before*
# any writing so a mid-run failure re-processes rather than skips.

# CELL ********************

# Read only what is new since the last successful run
wm_fire = get_watermark("silver_fire_detections")
wm_wx   = get_watermark("silver_weather")
wm_air  = get_watermark("silver_aircraft")

new_fire = src("silver_fire_detections").where(F.col("ingest_ts") > F.lit(wm_fire)).cache()
new_wx   = src("silver_weather").where(F.col("ingest_ts") > F.lit(wm_wx)).cache()
new_air  = src("silver_aircraft").where(F.col("ingest_ts") > F.lit(wm_air)).cache()

n_fire, n_wx, n_air = new_fire.count(), new_wx.count(), new_air.count()
print(f"new detections : {n_fire}")
print(f"new weather    : {n_wx}")
print(f"new aircraft   : {n_air}")

# Highest ingest_ts actually observed, committed at the end of the run.
max_fire = new_fire.agg(F.max("ingest_ts")).head()[0] if n_fire else None
max_wx   = new_wx.agg(F.max("ingest_ts")).head()[0]   if n_wx   else None
max_air  = new_air.agg(F.max("ingest_ts")).head()[0]  if n_air  else None

if n_fire == 0 and n_wx == 0 and n_air == 0:
    print("\nNothing new since the last run. Entity refresh still proceeds "
          "so the active fire set can age out.")

# MARKDOWN ********************

# ## Merge helper
# 
# Every incremental write goes through one place, so create-on-first-run and
# merge-thereafter behave identically everywhere.

# CELL ********************

# Upsert helper used by every incremental table
def upsert(df, table, keys, partition=None):
    """Create the table on first run, otherwise merge on the key columns."""
    full = dst_name(table)
    if df is None:
        print(f"  {table:<26} skipped (no frame)")
        return 0
    n = df.count()
    if not spark.catalog.tableExists(full):
        w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition:
            w = w.partitionBy(partition)
        w.saveAsTable(full)
        print(f"  {table:<26} created, {n} rows")
        return n
    if n == 0:
        print(f"  {table:<26} no change")
        return 0
    cond = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    (DeltaTable.forName(spark, full).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  {table:<26} merged {n} rows")
    return n

def overwrite(df, table, schema=None, partition=None):
    """Full replace, for the bounded active-set tables."""
    full = dst_name(table)
    if df is None or df.rdd.isEmpty():
        if schema is None:
            print(f"  {table:<26} skipped (empty, no schema)")
            return 0
        df = spark.createDataFrame([], schema)
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition:
        w = w.partitionBy(partition)
    w.saveAsTable(full)
    n = df.count()
    print(f"  {table:<26} overwritten, {n} rows")
    return n

# MARKDOWN ********************

# ## Reference entities
# 
# Static geography. Merged on the natural key so a change in the source is picked
# up without rewriting 34k rows every five minutes.

# CELL ********************

# Reference entities: merge on natural key
print("REFERENCE ENTITIES")

communes = (src("ref_communes")
            .select("commune_id", "commune", "dept", "region",
                    "population", "latitude", "longitude"))
stations = (src("ref_fire_stations")
            .select("station_id", "name", "operator",
                    "latitude", "longitude", "station_type"))
water    = (src("ref_water_bodies")
            .select("water_id", "name", "water_type",
                    "latitude", "longitude", "scoopable"))
infra    = (src("ref_critical_infra")
            .select("infra_id", "name", "infra_type", "latitude", "longitude"))

upsert(communes, "ent_commune",        ["commune_id"])
upsert(stations, "ent_fire_station",   ["station_id"])
upsert(water,    "ent_water_body",     ["water_id"])
upsert(infra,    "ent_critical_infra", ["infra_id"])

# MARKDOWN ********************

# ## Current-state entities
# 
# `ent_aircraft` and `ent_weather_grid` mirror the `arg_max` materialized views.
# Because the merge key is the entity id, taking the latest row **per key from the
# new rows only** and merging gives the same result as re-scanning all history.

# CELL ********************

# Aircraft and weather grid: latest row per key, from new rows only
from pyspark.sql import Window

print("CURRENT-STATE ENTITIES")

if n_air:
    w_air = Window.partitionBy("icao24").orderBy(F.col("ingest_ts").desc())
    latest_air = (new_air
        .withColumn("_rn", F.row_number().over(w_air))
        .where(F.col("_rn") == 1).drop("_rn")
        .select("icao24", "callsign", "aircraft_role", "latitude", "longitude",
                "altitude_m", "velocity_ms", "heading_deg", "on_ground",
                "is_firefighting", "ingest_ts"))
    upsert(latest_air, "ent_aircraft", ["icao24"])
else:
    print("  ent_aircraft               no new rows")

if n_wx:
    w_wx = Window.partitionBy("grid_id").orderBy(F.col("observed_at").desc())
    latest_wx = (new_wx
        .withColumn("_rn", F.row_number().over(w_wx))
        .where(F.col("_rn") == 1).drop("_rn")
        .select("grid_id", "latitude", "longitude", "temperature_c",
                "humidity_pct", "wind_speed_kmh", "wind_direction_deg",
                "vpd_kpa", "pm2_5", "observed_at"))
    upsert(latest_wx, "ent_weather_grid", ["grid_id"])
else:
    print("  ent_weather_grid           no new rows")

# MARKDOWN ********************

# ## Time series
# 
# Unbounded tables, so history is never rescanned. Merging on `(key, timestamp)`
# rather than appending makes a re-run idempotent, which matters because a failed
# run leaves the watermark unmoved and the next run replays the same window.

# CELL ********************

# Time-series tables: merge new rows on (key, timestamp)
print("TIME SERIES")

if n_air:
    ts_air = new_air.select(
        F.col("icao24"),
        F.col("ingest_ts").alias("timestamp"),
        "latitude", "longitude", "altitude_m", "velocity_ms",
        "heading_deg", "on_ground", "is_firefighting")
    upsert(ts_air, "ts_aircraft_positions", ["icao24", "timestamp"])
else:
    print("  ts_aircraft_positions      no new rows")

if n_wx:
    ts_wx = new_wx.select(
        F.col("grid_id"),
        F.col("observed_at").alias("timestamp"),
        "temperature_c", "humidity_pct", "wind_speed_kmh",
        "wind_direction_deg", "vpd_kpa", "pm2_5")
    upsert(ts_wx, "ts_weather_observations", ["grid_id", "timestamp"])
else:
    print("  ts_weather_observations    no new rows")

# MARKDOWN ********************

# ## Fire zone frames
# 
# Reproduces `gold_fire_zones`: detections binned into 10 minute frames per S2
# cell. A late arriving detection can land in a frame that was already written, so
# the affected frames are recomputed from the full silver shortcut rather than
# appended blindly, then merged.

# CELL ********************

# ts_fire_zone_signals: recompute only the frames touched by new detections
print("FIRE ZONE FRAMES")

lookback_floor = RUN_TS - dt.timedelta(hours=LOOKBACK_H)

frame_expr = (F.from_unixtime(
    (F.unix_timestamp("acq_datetime") / (FRAME_MIN * 60)).cast("long") * (FRAME_MIN * 60)
).cast("timestamp"))

if n_fire:
    # Which cells moved? Only those need their frames rebuilt.
    dirty_cells = [r["s2_cell"] for r in
                   new_fire.select("s2_cell").distinct().collect()]
    print(f"  cells touched: {len(dirty_cells)}")

    all_fire = (src("silver_fire_detections")
                .where(F.col("acq_datetime") >= F.lit(lookback_floor))
                .where(F.col("s2_cell").isin(dirty_cells)))

    ts_fire = (all_fire
        .withColumn("timestamp", frame_expr)
        .groupBy(F.col("s2_cell").alias("fire_zone_id"), "timestamp")
        .agg(F.sum("frp").alias("frp_total"),
             F.max("frp").alias("frp_max"),
             F.countDistinct("detection_id").alias("detections"),
             F.avg("latitude").alias("latitude"),
             F.avg("longitude").alias("longitude")))

    upsert(ts_fire, "ts_fire_zone_signals", ["fire_zone_id", "timestamp"])
else:
    print("  ts_fire_zone_signals       no new detections")

# MARKDOWN ********************

# ## Active fire zones
# 
# The active set is bounded and zones must disappear once they stop burning, so
# this is a full recompute over the lookback window rather than a merge. Weather
# is attached from the nearest grid point, and the spread vector and threat score
# follow the same formulation as notebook 05.

# CELL ********************

# Rebuild the active fire zone entity over the lookback window
import numpy as np, pandas as pd

print("ACTIVE FIRE ZONES")

zones_sdf = (src("silver_fire_detections")
    .where(F.col("acq_datetime") >= F.lit(lookback_floor))
    .withColumn("frame", frame_expr)
    .groupBy("s2_cell")
    .agg(F.sum("frp").alias("frp_total"),
         F.max("frp").alias("frp_max"),
         F.countDistinct("detection_id").alias("detections"),
         F.avg("latitude").alias("latitude"),
         F.avg("longitude").alias("longitude"),
         F.min("acq_datetime").alias("first_seen"),
         F.max("acq_datetime").alias("last_seen"),
         F.countDistinct("frame").alias("frames"),
         F.concat_ws(",", F.array_distinct(F.collect_list("satellite"))).alias("satellites"))
    .where(F.col("frp_max") >= MIN_FRP))

fire_zones = zones_sdf.toPandas()
print(f"  active zones: {len(fire_zones)}")

communes_pd = communes.toPandas()
stations_pd = stations.toPandas()
water_pd    = water.toPandas()

# Latest weather per grid, read back from the entity table so this does not
# depend on whether the current run happened to bring new weather rows.
if spark.catalog.tableExists(dst_name("ent_weather_grid")):
    weather_pd = spark.table(dst_name("ent_weather_grid")).toPandas()
else:
    weather_pd = pd.DataFrame()
print(f"  weather grid points: {len(weather_pd)}")

# MARKDOWN ********************

# ## Geospatial helpers
# 
# Local-tangent-plane approximations. Over the tens of kilometres that matter for
# a fire they are accurate to well under a percent, and they vectorise cleanly.

# CELL ********************

# Vectorised geo helpers
R_EARTH_KM = 6371.0

def km_between(lat1, lon1, lat2, lon2):
    p1, p2 = np.radians(lat1), np.radians(lat2)
    dp = p2 - p1
    dl = np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dp / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2) ** 2
    return 2 * R_EARTH_KM * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

def bearing_deg(lat1, lon1, lat2, lon2):
    dlon = np.radians(np.asarray(lon2) - np.asarray(lon1))
    p1, p2 = np.radians(lat1), np.radians(lat2)
    y = np.sin(dlon) * np.cos(p2)
    x = np.cos(p1) * np.sin(p2) - np.sin(p1) * np.cos(p2) * np.cos(dlon)
    return (np.degrees(np.arctan2(y, x)) + 360.0) % 360.0

def angle_gap(a, b):
    return np.abs((np.asarray(a) - np.asarray(b) + 540.0) % 360.0 - 180.0)

def nearest(df_from, df_to, prefix, id_col, name_col=None):
    if df_from.empty or df_to.empty:
        df_from[f"{prefix}_id"] = None
        df_from[f"{prefix}_km"] = None
        if name_col:
            df_from[f"{prefix}_name"] = None
        return df_from
    tl, tn = df_to.latitude.values, df_to.longitude.values
    ids, kms, names = [], [], []
    for lat, lon in zip(df_from.latitude.values, df_from.longitude.values):
        d = km_between(lat, lon, tl, tn)
        i = int(np.argmin(d))
        ids.append(df_to.iloc[i][id_col])
        kms.append(round(float(d[i]), 2))
        if name_col:
            names.append(df_to.iloc[i][name_col])
    df_from[f"{prefix}_id"] = ids
    df_from[f"{prefix}_km"] = kms
    if name_col:
        df_from[f"{prefix}_name"] = names
    return df_from

# MARKDOWN ********************

# ## Enrich the zones
# 
# Weather attachment, spread vector and fire status. The spread rate is a
# Rothermel-flavoured heuristic: wind dominates, dryness scales it.

# CELL ********************

# Attach weather, derive the spread vector and status
if not fire_zones.empty:
    if not weather_pd.empty:
        wlat, wlon = weather_pd.latitude.values, weather_pd.longitude.values
        idx = [int(np.argmin(km_between(la, lo, wlat, wlon)))
               for la, lo in zip(fire_zones.latitude, fire_zones.longitude)]
        for col in ["temperature_c", "humidity_pct", "wind_speed_kmh",
                    "wind_direction_deg", "vpd_kpa", "pm2_5"]:
            fire_zones[col] = weather_pd.iloc[idx][col].values
        fire_zones["weather_grid_id"] = weather_pd.iloc[idx]["grid_id"].values
    else:
        for col in ["temperature_c", "humidity_pct", "wind_speed_kmh",
                    "wind_direction_deg", "vpd_kpa", "pm2_5", "weather_grid_id"]:
            fire_zones[col] = None

    fire_zones["intensity_per_frame"] = (
        fire_zones.frp_total / fire_zones.frames.clip(lower=1)).round(2)

    w   = fire_zones.wind_speed_kmh.fillna(0).astype(float)
    rh  = fire_zones.humidity_pct.fillna(60).astype(float)
    vpd = fire_zones.vpd_kpa.fillna(1.0).astype(float)
    dry = ((100.0 - rh) / 100.0) * (1.0 + vpd / 3.0)

    fire_zones["spread_rate_kmh"] = (0.05 + 0.045 * w * dry).round(3)
    fire_zones["spread_heading_deg"] = (
        (fire_zones.wind_direction_deg.fillna(0) + 180.0) % 360.0).round(0)
    fire_zones["projected_reach_km"] = (fire_zones.spread_rate_kmh * 6.0).round(2)

    fire_zones["fire_zone_id"] = fire_zones.s2_cell.astype(str)
    fire_zones["status"] = np.where(
        fire_zones.frp_max >= 50, "MAJOR",
        np.where(fire_zones.frp_max >= 15, "ACTIVE", "SMOULDERING"))

    print(fire_zones[["fire_zone_id", "frp_max", "frames", "wind_speed_kmh",
                      "spread_rate_kmh", "spread_heading_deg", "status"]]
          .head(10).to_string(index=False))
else:
    print("  no active zones in the lookback window")

# MARKDOWN ********************

# ## Bridge tables
# 
# The ontology can only join on key equality, so every spatial relationship is
# resolved to explicit id pairs here. Distance cannot live on the ontology edge,
# so it is also denormalised onto the entity rows in the next cell.

# CELL ********************

# Spatial bridges, recomputed for the current active set
print("BRIDGES")

rows_fc, rows_sc, rows_fs, rows_aw, rows_af = [], [], [], [], []

if not fire_zones.empty:
    clat, clon = communes_pd.latitude.values, communes_pd.longitude.values
    slat, slon = stations_pd.latitude.values, stations_pd.longitude.values
    wl,   wn   = water_pd.latitude.values,    water_pd.longitude.values

    for _, fz in fire_zones.iterrows():
        fid, flat, flon = fz.fire_zone_id, fz.latitude, fz.longitude
        wind_dir = fz.wind_direction_deg

        # FireZone THREATENS Commune
        d = km_between(flat, flon, clat, clon)
        for i in np.where(d <= THREAT_KM)[0]:
            b = float(bearing_deg(flat, flon, clat[i], clon[i]))
            downwind = bool(wind_dir is not None and not pd.isna(wind_dir)
                            and angle_gap(b, (float(wind_dir) + 180.0) % 360.0) <= 45.0)
            rows_fc.append({"fire_zone_id": fid,
                            "commune_id": communes_pd.iloc[i].commune_id,
                            "distance_km": round(float(d[i]), 2),
                            "bearing_deg": round(b, 1),
                            "is_downwind": downwind,
                            "population": int(communes_pd.iloc[i].population)})

        # FireZone SERVED_BY FireStation
        ds = km_between(flat, flon, slat, slon)
        for i in np.where(ds <= STATION_KM)[0]:
            rows_fs.append({"fire_zone_id": fid,
                            "station_id": stations_pd.iloc[i].station_id,
                            "distance_km": round(float(ds[i]), 2)})

        # FireZone REFILL_AT WaterBody, scoopable only, five nearest
        dw = km_between(flat, flon, wl, wn)
        cand = [i for i in np.where(dw <= WATER_KM)[0] if bool(water_pd.iloc[i].scoopable)]
        cand.sort(key=lambda i: dw[i])
        for i in cand[:5]:
            rows_aw.append({"fire_zone_id": fid,
                            "water_id": water_pd.iloc[i].water_id,
                            "distance_km": round(float(dw[i]), 2)})

    # FireStation COVERS Commune, nearest station per commune
    for _, c in communes_pd.iterrows():
        dd = km_between(c.latitude, c.longitude, slat, slon)
        i = int(np.argmin(dd))
        if dd[i] <= STATION_KM:
            rows_sc.append({"station_id": stations_pd.iloc[i].station_id,
                            "commune_id": c.commune_id,
                            "distance_km": round(float(dd[i]), 2)})

    # Aircraft ASSIGNED_TO FireZone, firefighting aircraft within 50 km
    if spark.catalog.tableExists(dst_name("ent_aircraft")):
        fa = (spark.table(dst_name("ent_aircraft"))
                   .where(F.col("is_firefighting") == True).toPandas())
        for _, a in fa.iterrows():
            d = km_between(a.latitude, a.longitude,
                           fire_zones.latitude.values, fire_zones.longitude.values)
            for i in np.where(d <= 50.0)[0]:
                rows_af.append({"icao24": a.icao24,
                                "fire_zone_id": fire_zones.iloc[i].fire_zone_id,
                                "distance_km": round(float(d[i]), 2)})

bridge_firezone_commune  = pd.DataFrame(rows_fc)
bridge_station_commune   = pd.DataFrame(rows_sc)
bridge_firezone_station  = pd.DataFrame(rows_fs)
bridge_firezone_water    = pd.DataFrame(rows_aw)
bridge_aircraft_firezone = pd.DataFrame(rows_af)

for n, d in [("firezone_commune", bridge_firezone_commune),
             ("station_commune", bridge_station_commune),
             ("firezone_station", bridge_firezone_station),
             ("firezone_water", bridge_firezone_water),
             ("aircraft_firezone", bridge_aircraft_firezone)]:
    print(f"  bridge_{n:<20} {len(d)}")

# MARKDOWN ********************

# ## Denormalise onto the fire zone
# 
# A Contextualization binds key columns only, so `THREATENS` cannot carry
# `distance_km`. Those figures are rolled onto the FireZone entity, which is what
# the Data Agent and the Activator actually read.

# CELL ********************

# Roll exposure figures onto the entity and score the threat
if not fire_zones.empty:
    if not bridge_firezone_commune.empty:
        agg = bridge_firezone_commune.groupby("fire_zone_id").agg(
            communes_at_risk=("commune_id", "nunique"),
            population_exposed=("population", "sum"),
            nearest_commune_km=("distance_km", "min"))
        dw = (bridge_firezone_commune[bridge_firezone_commune.is_downwind]
              .groupby("fire_zone_id")["population"].sum().rename("population_downwind"))
        near = (bridge_firezone_commune.sort_values("distance_km")
                .groupby("fire_zone_id").first()["commune_id"].rename("nearest_commune_id"))
        fire_zones = (fire_zones
            .merge(agg,  left_on="fire_zone_id", right_index=True, how="left")
            .merge(dw,   left_on="fire_zone_id", right_index=True, how="left")
            .merge(near, left_on="fire_zone_id", right_index=True, how="left"))

    for col, default in [("communes_at_risk", 0), ("population_exposed", 0),
                         ("population_downwind", 0), ("nearest_commune_km", 999.0)]:
        if col not in fire_zones.columns:
            fire_zones[col] = default
        fire_zones[col] = fire_zones[col].fillna(default)
    if "nearest_commune_id" not in fire_zones.columns:
        fire_zones["nearest_commune_id"] = None

    fire_zones = nearest(fire_zones, stations_pd, "nearest_station", "station_id", "name")
    scoop = water_pd[water_pd.scoopable == True]
    fire_zones = nearest(fire_zones, scoop, "nearest_water", "water_id", "name")

    fire_zones["threat_score"] = (
        fire_zones.frp_total.astype(float)
        * np.log10(fire_zones.population_exposed.astype(float) + 10.0)
        * (1.0 + fire_zones.population_downwind.astype(float)
                 / (fire_zones.population_exposed.astype(float) + 1.0))
        * (1.0 + fire_zones.spread_rate_kmh.astype(float) * 2.0)
    ).round(1)

    fire_zones["severity"] = np.select(
        [fire_zones.threat_score >= 400, fire_zones.threat_score >= 200,
         fire_zones.threat_score >= 80],
        ["CRITICAL", "HIGH", "MODERATE"], default="LOW")

    fire_zones["updated_at"] = RUN_TS

    print(fire_zones[["fire_zone_id", "frp_total", "population_exposed",
                      "population_downwind", "threat_score", "severity"]]
          .sort_values("threat_score", ascending=False).head(10).to_string(index=False))

# MARKDOWN ********************

# ## Write the active set
# 
# Empty bridges still need to exist with the right schema, otherwise the ontology
# relationship that binds to them cannot resolve. Aerial assets are frequently all
# on the ground, so an empty `bridge_aircraft_firezone` is the normal case rather
# than an error.

# CELL ********************

# Overwrite the bounded tables
EMPTY = {
  "bridge_aircraft_firezone": T.StructType([
      T.StructField("icao24", T.StringType()),
      T.StructField("fire_zone_id", T.StringType()),
      T.StructField("distance_km", T.DoubleType())]),
  "bridge_firezone_commune": T.StructType([
      T.StructField("fire_zone_id", T.StringType()),
      T.StructField("commune_id", T.StringType()),
      T.StructField("distance_km", T.DoubleType()),
      T.StructField("bearing_deg", T.DoubleType()),
      T.StructField("is_downwind", T.BooleanType()),
      T.StructField("population", T.LongType())]),
  "bridge_firezone_station": T.StructType([
      T.StructField("fire_zone_id", T.StringType()),
      T.StructField("station_id", T.StringType()),
      T.StructField("distance_km", T.DoubleType())]),
  "bridge_firezone_water": T.StructType([
      T.StructField("fire_zone_id", T.StringType()),
      T.StructField("water_id", T.StringType()),
      T.StructField("distance_km", T.DoubleType())]),
  "bridge_station_commune": T.StructType([
      T.StructField("station_id", T.StringType()),
      T.StructField("commune_id", T.StringType()),
      T.StructField("distance_km", T.DoubleType())]),
}

def to_sdf(pdf, table):
    if pdf is None or len(pdf) == 0:
        return spark.createDataFrame([], EMPTY[table]) if table in EMPTY else None
    return spark.createDataFrame(pdf)

print("ACTIVE SET")
if not fire_zones.empty:
    overwrite(spark.createDataFrame(fire_zones), "ent_fire_zone")
else:
    print("  ent_fire_zone              left unchanged (no active zones)")

for tbl, pdf in [("bridge_firezone_commune",  bridge_firezone_commune),
                 ("bridge_station_commune",   bridge_station_commune),
                 ("bridge_firezone_station",  bridge_firezone_station),
                 ("bridge_firezone_water",    bridge_firezone_water),
                 ("bridge_aircraft_firezone", bridge_aircraft_firezone)]:
    overwrite(to_sdf(pdf, tbl), tbl, schema=EMPTY.get(tbl))

# MARKDOWN ********************

# ## Commit the watermarks
# 
# Last step on purpose. If anything above fails the watermarks stay put and the
# next run replays the same window, which the merges absorb without duplicating.

# CELL ********************

# Advance the watermarks only after everything above succeeded
set_watermark("silver_fire_detections", max_fire, n_fire)
set_watermark("silver_weather",         max_wx,   n_wx)
set_watermark("silver_aircraft",        max_air,  n_air)

spark.table(WM_TABLE).orderBy("source_table").show(truncate=False)

# MARKDOWN ********************

# ## Verify

# CELL ********************

# Row counts across the ontology base
tables = ["ent_fire_zone", "ent_commune", "ent_fire_station", "ent_water_body",
          "ent_critical_infra", "ent_weather_grid", "ent_aircraft",
          "ts_fire_zone_signals", "ts_aircraft_positions", "ts_weather_observations",
          "bridge_firezone_commune", "bridge_station_commune",
          "bridge_firezone_station", "bridge_firezone_water",
          "bridge_aircraft_firezone"]

print(f"{DST_FQ} contents")
for t in tables:
    full = dst_name(t)
    if spark.catalog.tableExists(full):
        print(f"  {t:<26} {spark.table(full).count()}")
    else:
        print(f"  {t:<26} MISSING")

elapsed = (dt.datetime.now(dt.timezone.utc).replace(tzinfo=None) - RUN_TS).total_seconds()
print(f"\nrun took {elapsed:.0f}s")

# Fail loudly rather than reporting success on an empty build. A schema-enabled
# lakehouse will happily accept a write to an unqualified name and put it
# nowhere useful, which looks identical to success from the job status.
missing = [t for t in tables if not spark.catalog.tableExists(dst_name(t))]
if missing:
    raise RuntimeError(f"ontology base incomplete, missing: {missing}")
if spark.table(dst_name("ent_commune")).count() == 0:
    raise RuntimeError("ent_commune is empty, reference load did not land")
print("all ontology tables present")

# MARKDOWN ********************

# ## Top threats right now

# CELL ********************

# Current picture, ranked
if spark.catalog.tableExists(dst_name("ent_fire_zone")):
    fz = spark.table(dst_name("ent_fire_zone")).toPandas()
    cm = spark.table(dst_name("ent_commune")).select("commune_id", "commune").toPandas()
    top = fz.sort_values("threat_score", ascending=False).head(5)
    for _, r in top.iterrows():
        cname = ""
        if r.get("nearest_commune_id") is not None:
            m = cm[cm.commune_id == r.nearest_commune_id]
            if len(m):
                cname = m.iloc[0].commune
        print(f"[{r.severity}] {cname or r.fire_zone_id}  score {r.threat_score}")
        print(f"    FRP {r.frp_total:.1f} MW over {int(r.frames)} frames, "
              f"{int(r.detections)} detections")
        print(f"    {int(r.population_exposed):,} exposed, "
              f"{int(r.population_downwind):,} downwind")
        print(f"    spread {r.spread_rate_kmh} km/h heading {r.spread_heading_deg:.0f} deg")
        print(f"    nearest station {r.nearest_station_name} at {r.nearest_station_km} km")
        print(f"    nearest scoopable water {r.nearest_water_name} at {r.nearest_water_km} km")
