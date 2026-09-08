# Documentation

## When the Wind Turns — presentation deck

**[Wildfire-Intelligence-When-the-Wind-Turns.pdf](Wildfire-Intelligence-When-the-Wind-Turns.pdf)** (24 slides)

A scenario-led walkthrough of this solution, built from live screenshots of the
deployed Fabric workspace. Every figure quoted in the deck was captured from a
running deployment on 8 September 2026 — not a mock-up.

### Structure

| Slides | Section | Content |
|---|---|---|
| 1–3 | The problem | Why "who is downwind?" is the question that matters, and why it normally crosses five systems |
| 4–12 | The demo | Workspace proof, satellite ingestion, live map, dashboard, ontology, graph, data agent, Activator |
| 13–17 | The architecture | Eventstream fan-out, Eventhouse medallion, OneLake availability, the engine stack |
| 18–20 | Trust and close | Observed vs inferred vs not established; the pattern beyond wildfires |
| 21–24 | Appendix | Run of show, click paths, pre-flight checklist, evidence index |

### Presenting this

Slide 21 is a 25-minute run of show. Slide 22 lists the exact workspace items to
open and in what order.

**Run the pre-flight checks on slide 23 before you present.** Two failure modes
will silently empty the dashboard:

- **`ES_Wildfire` destinations can sit `Inactive` while the source shows `Active`.**
  Events are accepted (HTTP 201) and never ingested. Open the Eventstream and
  confirm all three destinations read `Active`.
- **`02_ingest_satellite_fires` fails when the NASA FIRMS map key hits its quota**
  (~5,000 transactions per 10 minutes). Check the Monitoring hub, filter to the
  workspace, and re-run the notebook manually if needed.

### Scope

The incident narrative is fictional. The implementation, the data sources and
every number in the deck are real.

Sources: NASA FIRMS, Open-Meteo, OpenSky Network, OpenStreetMap.
