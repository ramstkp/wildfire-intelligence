# wildfire-intelligence

Real-time wildfire detection and dispatch intelligence on Microsoft Fabric: NASA FIRMS satellite thermal anomalies, fire weather and aircraft streamed through Eventstream into an Eventhouse medallion (bronze/silver/gold), with KQL dispatch functions, a Real-Time Dashboard, ontology, Data Agents and a graph model.

## Documentation

📄 **[When the Wind Turns](docs/Wildfire-Intelligence-When-the-Wind-Turns.pdf)** — a 24-slide walkthrough of this solution, built from live screenshots of the deployed workspace. Includes architecture, a 25-minute run of show, click paths and a pre-flight checklist. See [`docs/`](docs/) for details.

---

## What is in this repository

This repository is the **Git-synced definition of a Fabric workspace**. It was
exported by Fabric's built-in Git integration, so every folder is a Fabric item
and the folder name *is* the item name.

| Item | Type | Role |
|---|---|---|
| `00_config` | Notebook | Resolves workspace, Eventstream, Eventhouse and Lakehouse ids at run time. Every other notebook starts with `%run 00_config`. |
| `01_reference_data` | Notebook | Loads communes, critical infrastructure, fire stations, water bodies, ground units and GDACS alerts. |
| `02_ingest_satellite_fires` | Notebook | NASA FIRMS geostationary (10-min) + polar (375 m) detections → Eventstream. |
| `03_ingest_fire_weather` | Notebook | Open-Meteo fire weather → Eventstream. |
| `04_ingest_aircraft` | Notebook | OpenSky ADS-B positions → Eventstream. |
| `06_ont_wildfires_curation` | Notebook | Curates `rt_fires` → `ont_wildfires` entity and bridge tables. |
| `07_graph_wildfire_snapshot` | Notebook | Builds the `graph_wildfires` node and edge snapshot. |
| `ES_Wildfire` | Eventstream | One custom endpoint, three `event_type` filters, three Eventhouse destinations. |
| `EH_Wildfire` | Eventhouse | 12 tables, 23 KQL functions, 4 materialized views — bronze / silver / gold. |
| `LH_WildFires` | Lakehouse | 12 OneLake shortcuts onto the Eventhouse, plus curated and graph schemas. |
| `ONT_Wildfire_Impact` | Ontology | 8 entity types, 8 relationship types. |
| `GRAPH_Wildfire_Impact` | Graph model | Node and edge Delta sources for traversal. |
| `GQS_Wildfire_Story` | Graph queryset | 11 saved traversals used in the demo. |
| `RTD_Wildfire_Command` | Real-Time Dashboard | 4 pages, 21 tiles, 30-second refresh. |
| `Wildfire Atlas - Live` | Map | 12 layers over KQL functions and Lakehouse geography. |
| `ACT_Wildfire` | Activator | Three rules: CRITICAL, growth > 40 %, single FRP > 50 MW. |
| `AGENT_Wildfire` / `wildfire_tracking_agent` | Data agents | Ontology-grounded and Lakehouse-grounded question answering. |

---

## Deploying into your own Fabric environment

> **Read this first.** Fabric's Git export writes *some* references as the
> placeholder `00000000-0000-0000-0000-000000000000`, which Fabric rewrites to
> the current workspace on sync. It writes **others as literal item GUIDs from
> the workspace this was exported from**, and those resolve nowhere else.
> Syncing this repo gives you every item, correctly defined, but several of them
> land **unbound**. Step 6 fixes that, and it is not optional.

### Prerequisites

| | |
|---|---|
| Capacity | Fabric F or P SKU, or a trial. Eventhouse and Activator do not run on a Pro workspace. |
| Permissions | Workspace **Admin** — you will connect Git and edit item bindings. |
| NASA FIRMS map key | Free from <https://firms.modaps.eosdis.nasa.gov/api/area/>. Arrives by email in minutes. Quota is roughly 5,000 transactions per 10 minutes. |
| OpenSky account | *Optional.* Blank means anonymous access (~400 credits/day), which is enough at a 5-minute cadence. |

### 1. Create the workspace

Create a **new, empty** workspace on a capacity that supports Real-Time
Intelligence. Do not deploy into a workspace that already has items — the sync
in step 3 expects to be the only writer.

### 2. Connect it to Git

**Workspace settings → Git integration** → connect to a fork or clone of this
repository, branch `main`, folder `/`.

If Fabric cannot reach GitHub directly in your tenant, mirror the repo into
Azure DevOps and connect that instead.

### 3. Sync

**Source control → Update all.**

This creates every item and applies
`EH_Wildfire.Eventhouse/.children/EH_Wildfire.KQLDatabase/DatabaseSchema.kql`,
so the 12 tables, 23 functions and 4 materialized views are built for you. Wait
for the Eventhouse to finish provisioning before moving on — it is the slowest
item, and everything else points at it.

> The `docs/` folder is not a Fabric item. Fabric ignores it, but it will appear
> as an unrecognised path in the Source control pane. Leave it alone.

### 4. Set your API key

Open **`00_config`** and edit the credentials cell:

```python
FIRMS_KEY = "your-firms-map-key"

# optional — blank is anonymous
OPENSKY_CLIENT_ID     = ""
OPENSKY_CLIENT_SECRET = ""
```

Also change `CONTACT_EMAIL` to a monitored alias. The OpenStreetMap Overpass API
throttles or blocks clients that do not identify themselves, and the placeholder
address will earn you an HTTP 429.

**For anything beyond a demo, use Key Vault instead.** Set `KEYVAULT_URI` and the
vault wins over the literals — no other notebook changes:

```python
KEYVAULT_URI = "https://<your-vault>.vault.azure.net/"
```

The identity running the notebook needs **Key Vault Secrets User**. Secret names
are in `SECRET_NAMES` in the same cell.

> `00_config` needs no other edits. It resolves the workspace, Eventstream,
> Eventstream source, Eventhouse query URI, KQL database and Lakehouse **by item
> name at run time**, so the notebooks are portable as long as you keep the
> names.

### 5. Load reference data

Run **`01_reference_data`** once, end to end. It populates `ref_communes`,
`ref_critical_infra`, `ref_fire_stations`, `ref_water_bodies`,
`ref_ground_units` and `ref_gdacs_alerts` in the Eventhouse.

Nothing downstream works without this — the dispatch functions join against it.

### 6. Rebind the items that point at the old workspace

Each of these still references item GUIDs from the workspace this repo was
exported from. Open each and repoint it. **Item names are unchanged, so in every
case you are selecting the same name inside your own workspace.**

| # | Item | What to fix |
|---|---|---|
| 1 | `LH_WildFires` | 12 OneLake shortcuts under `Tables/rt_fires` point at the old KQL database. Delete and recreate them against **your** `EH_Wildfire`, keeping the same 12 names. |
| 2 | `ES_Wildfire` | Destinations `dst_fire`, `dst_weather`, `dst_aircraft` point at the old Eventhouse. Edit each, reselect `EH_Wildfire`, keep target tables `bronze_fire_raw` / `bronze_weather_raw` / `bronze_aircraft_raw` and **Processed ingestion**. |
| 3 | `RTD_Wildfire_Command` | The `EH_Wildfire` data source carries a stale `databaseArtifactId`. **Manage → Data sources** → reselect your KQL database. |
| 4 | `Wildfire Atlas - Live` | Layer sources reference the old KQL database and Lakehouse. Reselect both in layer settings. |
| 5 | `QS_Wildfire_Mirroring` | Hard-codes the old cluster URI. Reconnect it to your Eventhouse. |
| 6 | `ACT_Wildfire` | Rules point at the old Eventhouse. Each rule → **Manage source** → repoint. Leave the rules **disabled** until step 9. |
| 7 | `GRAPH_Wildfire_Impact` | **The awkward one.** `dataSources.json` stores fully-qualified `abfss://` paths containing the source workspace and Lakehouse GUIDs — no placeholder at all. Repoint all 15 node and edge sources at `LH_WildFires/Tables/graph_wildfires/…`. Do this *after* step 8. |
| 8 | `ONT_Wildfire_Impact` | 8 entity data bindings reference the old Lakehouse. Reselect `LH_WildFires` on each. Do this *after* step 8. |
| 9 | `AGENT_Wildfire`, `wildfire_tracking_agent` | Data sources reference the old ontology and Lakehouse. Reselect, then **Publish** each agent. |

Items 7, 8 and 9 need the curated tables to exist, so finish step 8 and come
back to them.

### 7. Start ingestion

Open **`ES_Wildfire`** → **Activate all** → **Now**.

Confirm the source **and all three destinations** read `Active`. Then run, in
order:

```
02_ingest_satellite_fires
03_ingest_fire_weather
04_ingest_aircraft
```

Give the Eventhouse two or three minutes, then check that `bronze_fire_raw` has
rows.

> **This is the failure mode that will cost you an afternoon.** An Eventstream
> source can show `Active` while every destination sits `Inactive`. Events are
> accepted with HTTP 201 and silently never ingested — no error surfaces
> anywhere. If the dashboard is empty, check the destinations before you check
> anything else.

### 8. Build the curated and graph layers

Run, in order:

```
06_ont_wildfires_curation     → LH_WildFires/Tables/ont_wildfires
07_graph_wildfire_snapshot    → LH_WildFires/Tables/graph_wildfires
```

Now go back and complete rebinding items 7, 8 and 9 in step 6.

### 9. Enable schedules and rules

Fabric exports schedules as **disabled**, so nothing runs until you switch it on.
Enable them per notebook:

| Notebook | Interval | Why |
|---|---|---|
| `02_ingest_satellite_fires` | 10 min | Matches the Met12 geostationary refresh. |
| `03_ingest_fire_weather` | 15 min | |
| `04_ingest_aircraft` | 5 min | |
| `06_ont_wildfires_curation` | 15 min | |
| `07_graph_wildfire_snapshot` | 15 min | The graph is only as fresh as its last snapshot. |

Then enable the three `ACT_Wildfire` rules and point their notifications at your
own Teams channel or mailbox.

### 10. Verify

You are done when all of these are true:

- [ ] `EH_Wildfire` → **Database details** shows **OneLake availability: Enabled** and 12/12 tables
- [ ] `ES_Wildfire` source **and** all three destinations read `Active`
- [ ] `RTD_Wildfire_Command` → **Situation right now** shows a non-zero count
- [ ] The dispatch table has rows with severity, growth % and a recommended action
- [ ] `Wildfire Atlas - Live` renders fire and infrastructure layers together
- [ ] `GQS_Wildfire_Story` → **`00 - Visual impact chain`** returns a populated graph
- [ ] `AGENT_Wildfire` answers *"which communes are most exposed to fire zones?"* with named communes
- [ ] Monitoring hub shows no failed runs for the workspace

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Dashboard shows `0` fires | Eventstream destinations inactive, or `02_ingest_satellite_fires` failing | Check the destinations first, then the Monitoring hub |
| `Invalid MAP_KEY` | Wrong or revoked FIRMS key | Re-issue from the FIRMS portal |
| FIRMS returns an empty body | Quota exhausted (~5,000 per 10 min) | Wait for the window to reset |
| Overpass returns 429 or 403 | `CONTACT_EMAIL` is still the placeholder | Set a real alias in `00_config` |
| `CapacityNotActive` | Capacity paused | Resume it in the Azure portal |
| `LookupError: <type> named '…' not found` | An item was renamed | Names are the contract — restore the original name, or update the constants at the top of `00_config` |
| Graph model returns nothing | Still bound to the old workspace, or `07` has not run | Rebind `dataSources.json`, then run `07_graph_wildfire_snapshot` |
| Ontology entities empty | Bindings point at the old Lakehouse | Rebind each entity to `LH_WildFires` |

---

## Notes on cost and data sources

- Ingestion at the cadences above is modest, but an Eventhouse on an idle
  capacity still burns CU. **Pause the capacity** when you are not demoing.
- NASA FIRMS, Open-Meteo, OpenSky Network and OpenStreetMap are public services
  with their own terms and rate limits. Respect them, identify your client
  honestly, and do not point production alerting at free tiers.
- Severity scores, growth percentages and time-to-impact here are **screening
  heuristics for demonstration**, not calibrated fire behaviour models. Slide 18
  of the deck sets out exactly what is observed, what is inferred and what is
  not established. Keep that distinction when you present.
