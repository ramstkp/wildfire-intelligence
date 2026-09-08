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

# # Wildfire relationships: observations, proximity and response context
# 
# This is a demonstration, not an incident-command system. Satellite thermal
# anomalies are not confirmed fire incidents or perimeters. Spatial links describe
# proximity, not damage, dispatch or operational availability.
# 
# The snapshot is anchored to the latest genuine satellite observation. When that
# observation is older than three hours, every node is marked HISTORICAL_NOT_LIVE.
# Aircraft and weather are selected as of that same anchor, avoiding a join between
# different days. Re-delivered FIRMS observations are deduplicated before aggregation.
# 
# Targets: `LH_WildFires.graph_wildfires` only. Existing ontology tables are untouched.


# CELL ********************

import datetime as dt
import json
import math
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T, Window

SOURCE = "LH_WildFires.rt_fires"
TARGET = "LH_WildFires.graph_wildfires"
NOW = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
spark.conf.set("spark.sql.session.timeZone", "UTC")

def source(name):
    return spark.table(f"{SOURCE}.{name}")

def coords(df):
    return df.where(F.col("latitude").between(-90, 90)
                    & F.col("longitude").between(-180, 180))

raw = coords(source("silver_fire_detections")).where(
    ~F.upper(F.coalesce(F.col("satellite"), F.lit(""))).contains("TEST")
    & ~F.upper(F.coalesce(F.col("instrument"), F.lit(""))).contains("TEST")
    & F.col("acq_datetime").isNotNull()
    & (F.col("acq_datetime") <= F.lit(NOW))
    & F.col("detection_id").isNotNull())
ANCHOR = raw.agg(F.max("acq_datetime")).first()[0]
if ANCHOR is None:
    raise RuntimeError("No genuine dated satellite observations: cannot build graph.")
MODE = "HISTORICAL_NOT_LIVE" if (NOW - ANCHOR).total_seconds() > 10800 else "RECENT_OBSERVATIONS"
SNAPSHOT = ANCHOR.isoformat() + "Z"
print(f"{MODE}: satellite snapshot {SNAPSHOT}; generated {NOW.isoformat()}Z")

# Match the Eventhouse dedup view: earliest delivery of each observation.
w = Window.partitionBy("detection_id").orderBy("ingest_ts")
fires = (raw.where(F.col("acq_datetime") >= F.lit(ANCHOR-dt.timedelta(hours=24)))
         .withColumn("_rn", F.row_number().over(w)).where("_rn=1").drop("_rn")
         .where(F.col("frp") >= 0).toPandas())
assert fires.detection_id.is_unique
fires["frame"] = pd.to_datetime(fires.acq_datetime).dt.floor("10min")

def reference(table, key):
    sdf = coords(source(table))
    if sdf.groupBy(key).count().where("count > 1").limit(1).count():
        raise RuntimeError(f"Duplicate reference keys in {table}")
    return sdf.toPandas()

communes = reference("ref_communes", "commune_id")
stations = reference("ref_fire_stations", "station_id")
water = reference("ref_water_bodies", "water_id")
infra = reference("ref_critical_infra", "infra_id")
if communes.empty or stations.empty:
    raise RuntimeError("Reference communes/stations missing.")

def latest_asof(table, key, timecol, age_minutes):
    sdf = coords(source(table)).where(
        (F.col(timecol) <= F.lit(ANCHOR))
        & (F.col(timecol) >= F.lit(ANCHOR-dt.timedelta(minutes=age_minutes))))
    win = Window.partitionBy(key).orderBy(F.col(timecol).desc(), F.col("ingest_ts").desc())
    return sdf.withColumn("_rn",F.row_number().over(win)).where("_rn=1").drop("_rn").toPandas()

weather = latest_asof("silver_weather","grid_id","observed_at",120)
aircraft = latest_asof("silver_aircraft","icao24","ingest_ts",30)
# All nearby airborne traffic is useful context. The inferred firefighting flag
# remains a property, not a reason to relabel commercial traffic as responders.
aircraft = aircraft[aircraft.on_ground.eq(False)]
frames = fires.groupby(["s2_cell","frame"],as_index=False).agg(
    FrameFrpMW=("frp","sum"), PeakDetectionMW=("frp","max"),
    Latitude=("latitude","mean"), Longitude=("longitude","mean"))
valid = set(frames.loc[frames.PeakDetectionMW >= 3.0,"s2_cell"])
fires = fires[fires.s2_cell.isin(valid)]
frames = frames[frames.s2_cell.isin(valid)]
print(f"{len(fires)} unique observations, {len(valid)} spatial cells; "
      f"{len(aircraft)} inferred aircraft and {len(weather)} weather points as of snapshot.")

# CELL ********************

def distance(lat,lon,lats,lons):
    p1,p2=np.radians(lat),np.radians(lats)
    a=np.sin((p2-p1)/2)**2 + np.cos(p1)*np.cos(p2)*np.sin(np.radians(lons-lon)/2)**2
    return 12742.0*np.arcsin(np.sqrt(np.clip(a,0,1)))

def bearing(lat,lon,lats,lons):
    a,b=np.radians(lat),np.radians(lats)
    dl=np.radians(lons-lon)
    return (np.degrees(np.arctan2(np.sin(dl)*np.cos(b),
              np.cos(a)*np.sin(b)-np.sin(a)*np.cos(b)*np.cos(dl)))+360)%360

def finite(v):
    return v is not None and pd.notna(v) and math.isfinite(float(v))

def iso(v):
    return pd.Timestamp(v).isoformat()+"Z" if pd.notna(v) else ""

nodes = {k: [] for k in ["FireZone","Detection","Commune","CriticalInfrastructure",
                          "Aircraft","Weather","FireStation","WaterBody"]}
edges = {k: [] for k in ["ObservedIn","NearCommune","DownwindScreen","NearInfrastructure",
                          "WeatherContext","NearbyAircraft","StationOption","ScoopOption"]}

def node(label,id,name,lat,lon,**props):
    row=dict(Id=str(id),Name=str(name or id),Latitude=float(lat),Longitude=float(lon),
             SnapshotTime=SNAPSHOT,Mode=MODE)
    row.update(props)
    nodes[label].append(row)
    return row

def edge(label,src,dst,km,basis):
    edges[label].append(dict(SourceId=str(src),TargetId=str(dst),
                            DistanceKm=round(float(km),3),Basis=basis,
                            SnapshotTime=SNAPSHOT,Mode=MODE))

used = {k:set() for k in ["Commune","CriticalInfrastructure","Weather","FireStation","WaterBody","Aircraft"]}
for cell, ff in frames.groupby("s2_cell"):
    ff=ff.sort_values("frame")
    last=ff.iloc[-1]
    obs=fires[fires.s2_cell.eq(cell)]
    lat,lon=float(last.Latitude),float(last.Longitude)
    dc=distance(lat,lon,communes.latitude.to_numpy(),communes.longitude.to_numpy())
    nearest=communes.iloc[int(np.argmin(dc))]
    # Growth is comparable only within the same geostationary satellite.
    geo=obs[obs.sensor_class.eq("geostationary")]
    growth=None
    if not geo.empty:
        sat=geo.groupby("satellite").acq_datetime.max().idxmax()
        series=geo[geo.satellite.eq(sat)].groupby("frame").frp.sum().sort_index()
        if len(series)>=2 and series.iloc[0]>0:
            growth=round(float((series.iloc[-1]/series.iloc[0]-1)*100),1)
    first,last_seen=obs.acq_datetime.min(),obs.acq_datetime.max()
    z=node("FireZone",cell,f"{nearest.commune} | {cell}",lat,lon,
           LastSeen=iso(last_seen),PeakDetectionMW=float(obs.frp.max()),
           LatestFrameMW=float(last.FrameFrpMW),DetectionCount=int(len(obs)),
           ObservationStatus="RECENT_IN_SNAPSHOT" if ANCHOR-last_seen<=dt.timedelta(hours=3)
                             else "OLDER_CONTEXT",
           GrowthPct=growth,
           Interpretation="S2 thermal-observation cell; not a confirmed incident/perimeter")
    nearest_w=None
    if not weather.empty:
        wd=distance(lat,lon,weather.latitude.to_numpy(),weather.longitude.to_numpy())
        wi=int(np.argmin(wd))
        if wd[wi] <= 75:
            nearest_w=weather.iloc[wi]
            edge("WeatherContext",cell,nearest_w.grid_id,wd[wi],
                 "Nearest available observation within 75km and 2h of snapshot")
            used["Weather"].add(str(nearest_w.grid_id))
    for i in np.where(dc<=30)[0]:
        c=communes.iloc[i]
        used["Commune"].add(str(c.commune_id))
        edge("NearCommune",cell,c.commune_id,dc[i],
             "Commune centroid within 30km; proximity only, not measured impact")
        if nearest_w is not None and finite(nearest_w.wind_direction_deg):
            to=(float(nearest_w.wind_direction_deg)+180)%360
            b=bearing(lat,lon,np.array([c.latitude]),np.array([c.longitude]))[0]
            if abs((b-to+540)%360-180)<=45:
                edge("DownwindScreen",cell,c.commune_id,dc[i],
                     "Illustrative 45deg sector around meteorological wind FROM+180; not plume forecast")
    di=distance(lat,lon,infra.latitude.to_numpy(),infra.longitude.to_numpy())
    for i in np.where(di<=10)[0]:
        r=infra.iloc[i]
        used["CriticalInfrastructure"].add(str(r.infra_id))
        edge("NearInfrastructure",cell,r.infra_id,di[i],
             "Reference asset point within 10km; no confirmed damage or disruption")
    ds=distance(lat,lon,stations.latitude.to_numpy(),stations.longitude.to_numpy())
    for i in np.argsort(ds)[:3]:
        if ds[i]<=40:
            r=stations.iloc[i]; used["FireStation"].add(str(r.station_id))
            edge("StationOption",cell,r.station_id,ds[i],
                 "One of 3 nearest reference stations within40km; availability unknown")
    dw=distance(lat,lon,water.latitude.to_numpy(),water.longitude.to_numpy())
    eligible=[i for i in np.argsort(dw) if dw[i]<=60 and water.iloc[i].scoopable==True][:3]
    for i in eligible:
        r=water.iloc[i]; used["WaterBody"].add(str(r.water_id))
        edge("ScoopOption",cell,r.water_id,dw[i],
             "Reference scoopable flag and distance only; access/safety unverified")
    for _,a in aircraft.iterrows():
        da=distance(lat,lon,np.array([a.latitude]),np.array([a.longitude]))[0]
        if da<=100:
            used["Aircraft"].add(str(a.icao24))
            edge("NearbyAircraft",a.icao24,cell,da,
                 "Airborne traffic within100km at snapshot; consult classification, not an assignment")

for _,r in fires.iterrows():
    node("Detection",r.detection_id,f"{r.satellite} | {iso(r.acq_datetime)}",
         r.latitude,r.longitude,ObservedAt=iso(r.acq_datetime),Satellite=str(r.satellite),
         SensorTier=str(r.sensor_class),FrpMW=float(r.frp))
    edge("ObservedIn",r.detection_id,r.s2_cell,0,"Deduplicated satellite observation in S2 cell")
for _,r in communes.iterrows():
    if str(r.commune_id) in used["Commune"]:
        node("Commune",r.commune_id,r.commune,r.latitude,r.longitude,
             Population=int(r.population) if finite(r.population) else 0,
             Department=str(r.dept),Interpretation="Population of entire commune, not affected-person estimate")
for label,df,key in [("CriticalInfrastructure",infra,"infra_id"),
                    ("FireStation",stations,"station_id"),("WaterBody",water,"water_id")]:
    for _,r in df.iterrows():
        if str(r[key]) in used[label]:
            kind=r.get("infra_type",r.get("station_type",r.get("water_type","")))
            node(label,r[key],r["name"],r.latitude,r.longitude,Category=str(kind or ""),
                 Interpretation="Reference location; operating status unverified")
for _,r in weather.iterrows():
    if str(r.grid_id) in used["Weather"]:
        node("Weather",r.grid_id,f"Weather {r.grid_id}",r.latitude,r.longitude,
             ObservedAt=iso(r.observed_at),
             WindFromDeg=float(r.wind_direction_deg) if finite(r.wind_direction_deg) else None,
             WindToDeg=(float(r.wind_direction_deg)+180)%360 if finite(r.wind_direction_deg) else None,
             WindKmh=float(r.wind_speed_kmh) if finite(r.wind_speed_kmh) else None,
             HumidityPct=float(r.humidity_pct) if finite(r.humidity_pct) else None)
for _,r in aircraft.iterrows():
    if str(r.icao24) not in used["Aircraft"]:
        continue
    node("Aircraft",r.icao24,str(r.callsign).strip() or r.icao24,r.latitude,r.longitude,
         ObservedAt=iso(r.ingest_ts),Role=str(r.aircraft_role),
         InferredFirefighting="yes" if r.is_firefighting==True else "no",
         HeadingDeg=float(r.heading_deg) if finite(r.heading_deg) else None,
         AltitudeM=float(r.altitude_m) if finite(r.altitude_m) else None,
         Interpretation="Inferred fleet classification; position is historical if Mode says so")

# CELL ********************

# Explicit schemas preserve stable types even when a source or edge set is empty.
COMMON = "Id string, Name string, Latitude double, Longitude double, SnapshotTime string, Mode string"
EXTRA = {
 "FireZone": "LastSeen string, PeakDetectionMW double, LatestFrameMW double, DetectionCount long, ObservationStatus string, GrowthPct double, Interpretation string",
 "Detection": "ObservedAt string, Satellite string, SensorTier string, FrpMW double",
 "Commune": "Population long, Department string, Interpretation string",
 "CriticalInfrastructure": "Category string, Interpretation string",
 "FireStation": "Category string, Interpretation string",
 "WaterBody": "Category string, Interpretation string",
 "Weather": "ObservedAt string, WindFromDeg double, WindToDeg double, WindKmh double, HumidityPct double",
 "Aircraft": "ObservedAt string, Role string, InferredFirefighting string, HeadingDeg double, AltitudeM double, Interpretation string",
}
ENDPOINTS = {
 "ObservedIn": ("Detection","FireZone"), "NearCommune": ("FireZone","Commune"),
 "DownwindScreen": ("FireZone","Commune"),
 "NearInfrastructure": ("FireZone","CriticalInfrastructure"),
 "WeatherContext": ("FireZone","Weather"), "NearbyAircraft": ("Aircraft","FireZone"),
 "StationOption": ("FireZone","FireStation"), "ScoopOption": ("FireZone","WaterBody"),
}
EDGE_SCHEMA="SourceId string, TargetId string, DistanceKm double, Basis string, SnapshotTime string, Mode string"
for label,rows in nodes.items():
    ids=[r["Id"] for r in rows]
    if len(ids)!=len(set(ids)):
        raise RuntimeError(f"Duplicate {label} node IDs")
for label,rows in edges.items():
    a,b=ENDPOINTS[label]
    aa={r["Id"] for r in nodes[a]}; bb={r["Id"] for r in nodes[b]}
    keys=[(r["SourceId"],r["TargetId"]) for r in rows]
    if len(keys)!=len(set(keys)) or any(x not in aa or y not in bb for x,y in keys):
        raise RuntimeError(f"Duplicate/orphan endpoints in {label}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET}")
manifest=[]
for label,rows in nodes.items():
    table="node_"+label.lower()
    sdf=spark.createDataFrame(rows,COMMON+", "+EXTRA[label])
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{TARGET}.{table}")
    manifest.append((table,int(len(rows)),SNAPSHOT,MODE,NOW.isoformat()+"Z"))
for label,rows in edges.items():
    table="edge_"+label.lower()
    sdf=spark.createDataFrame(rows,EDGE_SCHEMA)
    sdf.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{TARGET}.{table}")
    manifest.append((table,int(len(rows)),SNAPSHOT,MODE,NOW.isoformat()+"Z"))
status=spark.createDataFrame(manifest,"TableName string, RowCount long, SnapshotTime string, Mode string, GeneratedAt string")
status.write.format("delta").mode("overwrite").saveAsTable(f"{TARGET}.snapshot_status")
status.show(30,truncate=False)
print("All keys and relationship endpoints checked. Population must be counted once per Commune.Id.")
