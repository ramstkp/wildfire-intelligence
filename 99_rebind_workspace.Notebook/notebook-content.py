# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # 99 — Rebind this deployment to the current workspace
# 
# Run this **once after every deployment into a new workspace**, and again any
# time you restore or clone the workspace.
# 
# ## Why this exists
# 
# Most items in this solution are portable. Fabric's Git export writes their
# workspace reference as the placeholder `00000000-0000-0000-0000-000000000000`,
# which the service rewrites to the current workspace on sync, and it maps item
# ids through the stable `logicalId` in each `.platform` file.
# 
# Two items are not portable, because Fabric stores their references as
# **absolute GUIDs with no placeholder form**:
# 
# | Item | Stored as | Portable? |
# |---|---|---|
# | `GRAPH_Wildfire_Impact` | 16 fully-qualified `abfss://<workspace-guid>@onelake.../<lakehouse-guid>/Tables/...` paths | No |
# | `QS_Wildfire_Mirroring` | The Eventhouse cluster URI as a literal string | No |
# 
# There is no declarative fix. The graph model rejects a name-based path with
# `GraphDataSourcePathInvalid: Workspace id segment is not a valid GUID`, and it
# rejects the zero-GUID placeholder for the same reason. The only way to make
# these resolve automatically is to resolve them **at run time** and write the
# result back — which is what this notebook does.
# 
# ## What it does
# 
# 1. Resolves the current workspace, Lakehouse and Eventhouse by **name**, using
#    the same contract as `00_config`.
# 2. Rewrites `GRAPH_Wildfire_Impact/dataSources.json` so every node and edge
#    source points at this workspace's Lakehouse.
# 3. Rewrites the `QS_Wildfire_Mirroring` cluster URI.
# 4. Reports anything it cannot safely automate.
# 
# It is **idempotent**: if a definition is already correct it is left alone, and
# no update call is made.
# 
# ## Safety
# 
# `DRY_RUN = True` by default — it will show you every change and write nothing.
# Set it to `False` to apply. A JSON backup of each definition is written to the
# Lakehouse under `Files/rebind_backups/` before anything is modified.


# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set to False to apply changes. Leave True to preview.
DRY_RUN = True

# Item names — the same contract 00_config uses.
GRAPH_NAME    = "GRAPH_Wildfire_Impact"
QUERYSET_NAME = "QS_Wildfire_Mirroring"

# Where backups are written before any update.
BACKUP_DIR = "Files/rebind_backups"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import time
from datetime import datetime, timezone

import notebookutils
import requests

FABRIC_HOST = "https://api.fabric.microsoft.com"
_TOKEN = None

# Preferred transport. sempy's FabricRestClient is the supported way to call the
# Fabric REST API from a notebook and manages its own auth. The raw
# notebookutils.credentials.getToken path returns intermittent HTTP 500s
# (INTERNAL_ERROR, Retriable:true) on this platform, so it is only a fallback.
try:
    import sempy.fabric as _sempy
    _client = _sempy.FabricRestClient()
    print("transport  sempy.fabric.FabricRestClient")
except Exception as _exc:                                  # noqa: BLE001
    _client = None
    print(f"transport  raw REST (sempy unavailable: {_exc})")


def _get_token(retries=8):
    """Fallback token acquisition: cached, retried, and self-reporting."""
    global _TOKEN
    if _TOKEN:
        return _TOKEN

    sources = []
    if "_token" in globals():
        sources.append(("00_config._token", globals()["_token"]))
    sources.append(("notebookutils",
                    lambda: notebookutils.credentials.getToken(FABRIC_HOST)))

    last = None
    for attempt in range(1, retries + 1):
        for label, fn in sources:
            try:
                value = fn()
                if value:
                    _TOKEN = value
                    return _TOKEN
            except Exception as exc:                       # noqa: BLE001
                last = exc
                print(f"  token attempt {attempt}/{retries} via {label}: "
                      f"{str(exc).strip().splitlines()[0][:120]}")
        if attempt < retries:
            time.sleep(min(2 ** attempt, 30))

    raise RuntimeError(f"could not acquire a Fabric token: {last}")


def _hdr():
    return {"Authorization": f"Bearer {_get_token()}",
            "Content-Type": "application/json"}


def _request(method, path, body=None):
    """Call the Fabric API. `path` is relative, e.g. '/v1/workspaces/...'."""
    if _client is not None:
        fn = getattr(_client, method.lower())
        return fn(path, json=body) if body is not None else fn(path)
    return requests.request(method, FABRIC_HOST + path,
                            headers=_hdr(), json=body, timeout=120)


def _as_path(url):
    """Turn an absolute operation URL into a path both transports accept."""
    marker = "api.fabric.microsoft.com"
    return url.split(marker, 1)[1] if marker in url else url


def _lro(method, path, body=None, timeout_s=180):
    """Call an endpoint that may return 202 + Location, and wait for the result."""
    r = _request(method, path, body)

    if r.status_code in (200, 201):
        return r.json() if r.text else {}
    if r.status_code != 202:
        raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")

    location = r.headers.get("Location") or r.headers.get("location")
    if not location:
        raise RuntimeError(f"202 with no Location header from {path}")
    op = _as_path(location)

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(2)
        poll = _request("GET", op)
        state = poll.json() if poll.text else {}
        status = state.get("status")
        if status == "Succeeded":
            result = _request("GET", op.rstrip("/") + "/result")
            return result.json() if result.text else {}
        if status == "Failed":
            raise RuntimeError(
                f"operation failed: {json.dumps(state.get('error', state))[:500]}")
    raise TimeoutError(f"operation did not finish within {timeout_s}s")


def get_definition(item_id):
    return _lro("POST", f"/v1/workspaces/{WS}/items/{item_id}/getDefinition")


def update_definition(item_id, parts):
    return _lro(
        "POST",
        f"/v1/workspaces/{WS}/items/{item_id}/updateDefinition?updateMetadata=false",
        {"definition": {"parts": parts}},
    )


def part(defn, suffix):
    for p in defn["definition"]["parts"]:
        if p["path"].endswith(suffix):
            return p
    return None


def decode(p):
    return json.loads(base64.b64decode(p["payload"]).decode("utf-8"))


def encode(p, obj):
    p["payload"] = base64.b64encode(
        json.dumps(obj, indent=1).encode("utf-8")).decode("ascii")
    p["payloadType"] = "InlineBase64"


def backup(name, defn):
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = f"{BACKUP_DIR}/{name}_{stamp}.json"
    notebookutils.fs.put(path, json.dumps(defn, indent=1), True)
    return path


print(f"workspace {WS}")
print(f"lakehouse {LH_NAME} {LH_ID}")
print(f"eventhouse {KQL_DB} {KQL_URI}")
print(f"mode      {'DRY RUN - nothing will be written' if DRY_RUN else 'APPLY'}")

# Fail fast and loudly here rather than midway through a rebind.
_probe = _request("GET", f"/v1/workspaces/{WS}")
print(f"api probe {_probe.status_code}")
if _probe.status_code != 200:
    raise RuntimeError(f"cannot reach the Fabric API: {_probe.status_code} {_probe.text[:300]}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1 — Graph model data sources
# 
# Rewrites the workspace and Lakehouse segments of every `abfss://` path while
# preserving the table path after `/Tables/`, so renaming a node or edge table
# in `07_graph_wildfire_snapshot` does not require editing this notebook.


# CELL ********************

TARGET_ROOT = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables"

graph_id = find_item(GRAPH_NAME, "GraphModel")
graph_def = get_definition(graph_id)

ds_part = part(graph_def, "dataSources.json")
if ds_part is None:
    raise RuntimeError(f"{GRAPH_NAME} has no dataSources.json part")

sources = decode(ds_part)
changes = []

for ds in sources["dataSources"]:
    old = ds["properties"]["path"]
    if "/Tables/" not in old:
        print(f"  ! {ds['name']}: unexpected path shape, skipping -> {old}")
        continue
    tail = old.split("/Tables/", 1)[1]
    new = f"{TARGET_ROOT}/{tail}"
    if new != old:
        changes.append((ds["name"], old, new))
        ds["properties"]["path"] = new

if not changes:
    print(f"{GRAPH_NAME}: already bound to this workspace, nothing to do")
else:
    print(f"{GRAPH_NAME}: {len(changes)} of {len(sources['dataSources'])} sources need rebinding\n")
    for name, old, new in changes[:3]:
        print(f"  {name}\n    from {old}\n    to   {new}")
    if len(changes) > 3:
        print(f"  ... and {len(changes) - 3} more")

    if DRY_RUN:
        print("\n  DRY RUN - not written")
    else:
        saved = backup(GRAPH_NAME, graph_def)
        print(f"\n  backup -> {saved}")
        encode(ds_part, sources)
        update_definition(graph_id, graph_def["definition"]["parts"])
        print(f"  rebound {len(changes)} data sources")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2 — KQL queryset cluster URI
# 
# The queryset stores the Eventhouse query endpoint as a literal. `00_config`
# already resolved the correct one for this workspace, so we substitute it.


# CELL ********************

try:
    qs_id = find_item(QUERYSET_NAME, "KQLQueryset")
except LookupError as exc:
    print(f"{QUERYSET_NAME}: not present, skipping ({exc})")
    qs_id = None

if qs_id:
    qs_def = get_definition(qs_id)
    qs_part = part(qs_def, "RealTimeQueryset.json")

    if qs_part is None:
        print(f"{QUERYSET_NAME}: no RealTimeQueryset.json part, skipping")
    else:
        qs = decode(qs_part)
        touched = 0

        def fix_uris(node):
            """Walk the queryset and replace any stale clusterUri / database."""
            global touched
            if isinstance(node, dict):
                if "clusterUri" in node and node["clusterUri"] not in ("", KQL_URI):
                    print(f"  clusterUri  {node['clusterUri']}  ->  {KQL_URI}")
                    node["clusterUri"] = KQL_URI
                    touched += 1
                if node.get("databaseName") not in (None, KQL_DB):
                    print(f"  database    {node['databaseName']}  ->  {KQL_DB}")
                    node["databaseName"] = KQL_DB
                    touched += 1
                for v in node.values():
                    fix_uris(v)
            elif isinstance(node, list):
                for v in node:
                    fix_uris(v)

        fix_uris(qs)

        if touched == 0:
            print(f"{QUERYSET_NAME}: already correct for this workspace")
        elif DRY_RUN:
            print(f"{QUERYSET_NAME}: {touched} value(s) would change - DRY RUN, not written")
        else:
            saved = backup(QUERYSET_NAME, qs_def)
            print(f"  backup -> {saved}")
            encode(qs_part, qs)
            update_definition(qs_id, qs_def["definition"]["parts"])
            print(f"{QUERYSET_NAME}: updated {touched} value(s)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3 — What still needs a human
# 
# These are bindings Fabric manages through its own UI. They are normally
# remapped correctly on sync, but this cell tells you where to look if something
# is empty after a deployment.


# CELL ********************

checks = [
    ("ES_Wildfire",
     "Eventstream",
     "Source AND all three destinations must read Active. A source can be Active "
     "while destinations sit Inactive - events return HTTP 201 and are never ingested."),
    ("LH_WildFires",
     "Lakehouse",
     "12 OneLake shortcuts under Tables/rt_fires should resolve to this workspace's "
     "EH_Wildfire. If they show as broken, delete and recreate them."),
    ("RTD_Wildfire_Command",
     "KQLDashboard",
     "Manage > Data sources > confirm EH_Wildfire points at this workspace."),
    ("ONT_Wildfire_Impact",
     "Ontology",
     "Entity data bindings should resolve to LH_WildFires in this workspace."),
    ("ACT_Wildfire",
     "Reflex",
     "Rules carry an inert metadata.workspaceId from the source workspace. Confirm "
     "each rule's source resolves, then enable the rules."),
]

print(f"{'item':<24}{'type':<16}{'present':<9}")
print("-" * 78)
for name, item_type, note in checks:
    try:
        find_item(name, item_type)
        state = "yes"
    except LookupError:
        state = "MISSING"
    print(f"{name:<24}{item_type:<16}{state:<9}")
    print(f"    {note}\n")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Done
# 
# If this ran with `DRY_RUN = True`, review the output above, set it to `False`
# and run again.
# 
# After applying, verify:
# 
# - `GQS_Wildfire_Story` → run **`00 - Visual impact chain`** and confirm the
#   graph is populated.
# - `RTD_Wildfire_Command` → **Situation right now** shows a non-zero count.
# 
# Backups of every definition this notebook modified are in
# `LH_WildFires/Files/rebind_backups/`.
