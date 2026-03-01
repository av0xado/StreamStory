# Stream Story Prototype

Local prototype for visualizing multi-year Spotify streaming history as a narrative dashboard.

## What this includes
- Offline preprocessing pipeline (`scripts/preprocess.js`) that turns raw export JSON into compact dashboard payloads.
- Visual dashboard (`index.html`, `styles.css`, `app.js`) with:
  - big-picture stats
  - long timeline arc
  - top artists and tracks
  - nostalgia chapters
  - year explorer
  - listening personality
  - one-stream wonders
  - comeback arcs

## 1) Prepare the data
Run preprocessing from the project root:

```bash
node scripts/preprocess.js
```

This reads `data/Streaming_History_*.json` and writes payloads to `processed/`.

Optional timezone override:

```bash
STREAM_STORY_TZ="Europe/Dublin" node scripts/preprocess.js
```

## 2) Run the dashboard locally
Serve the project with a local HTTP server (required for `fetch`):

```bash
python3 -m http.server 4173
```

Open:

`http://localhost:4173`

## Prototype data contract
Generated files in `processed/`:
- `summary.json`
- `timeline_monthly.json`
- `top_artists.json`
- `top_tracks.json`
- `yearly_breakdown.json`
- `chapters.json`
- `one_stream_wonders.json`
- `comeback_arcs.json`
- `personality.json`
- `manifest.json`
