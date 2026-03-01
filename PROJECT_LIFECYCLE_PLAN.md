# Stream Story Project Lifecycle Plan

## 1) Prototype (Local, Single Dataset, Build Today)

### 1.1 Objectives
- Build a compelling, narrative dashboard that works locally against the existing Spotify export in `data/`.
- Prioritize speed of iteration and visual storytelling over production hardening.
- Prove the core concept: static + preprocessed analytics + creative story cards.

### 1.2 Scope
- Input: existing JSON files in `data/`.
- Runtime: local browser + local scripts only.
- Users: single developer/local use.
- Persistence: none required beyond local generated files.

### 1.3 Target Architecture (Prototype)
- `raw data` (Spotify JSON files in `data/`) -> `preprocess script` -> `small dashboard payload JSON` -> `frontend dashboard`.
- Preprocessing runs manually from CLI.
- Dashboard reads prepared payload files (not full raw export) to stay fast.

### 1.4 Deliverables
- `scripts/preprocess.(js|ts|sql)` to:
  - Normalize timestamps and event types.
  - Filter low-signal plays.
  - Generate aggregate payloads for dashboard sections.
- `public/processed/*.json` (or equivalent local folder) with:
  - `summary.json`
  - `timeline_monthly.json`
  - `top_artists.json`
  - `top_tracks.json`
  - `yearly_breakdown.json`
  - `chapters.json`
  - `one_stream_wonders.json`
  - `comeback_arcs.json`
  - `personality.json`
- Frontend pages/components rendering:
  - big picture stats
  - timeline arc
  - top artists/tracks
  - nostalgia chapters
  - year explorer
  - one-stream wonders
  - comeback arcs
  - listening personality
- A short local run guide in `README.md`.

### 1.5 Data Definitions to Lock Early
- `qualified_stream_track_ms`: e.g. >= 30,000 ms.
- `qualified_stream_episode_ms`: e.g. >= 180,000 ms.
- `timezone_for_analysis`: local (Europe/Dublin for your own export in prototype).
- Canonical IDs:
  - track: `spotify_track_uri`, fallback `track_name|artist`.
  - episode: `spotify_episode_uri`, fallback `episode_name|show_name`.
- Season mapping:
  - Spring (Mar-May), Summer (Jun-Aug), Autumn (Sep-Nov), Winter (Dec-Feb).

### 1.6 Narrative Metrics (Prototype)
- Core:
  - total listening hours
  - unique artists/tracks
  - yearly and monthly listening totals
  - top artists/tracks (by streams and by time)
  - skip/shuffle/offline rates
- Story-driven:
  - 1-stream wonders (single qualified play)
  - seasonal obsession chapters (artist dominates season/year)
  - comeback arcs (long inactivity gap then strong return)
  - discovery bursts (months with high unique-track novelty)
  - night-owl/early-bird tendency
  - “era labels” per season/year (heuristic generated titles)

### 1.7 Implementation Steps (Prototype)
1. Create repo structure for app + scripts (`src/`, `scripts/`, `public/processed/`).
2. Implement preprocessing script and run it against current data.
3. Validate aggregate outputs with spot checks.
4. Build dashboard UI from prepared payloads.
5. Add loading/error states and empty-state handling.
6. Tune copy and card text for narrative quality.
7. Manual QA across desktop/mobile.
8. Freeze prototype version and document decisions.

### 1.8 Acceptance Criteria (Prototype)
- Dashboard loads quickly on local machine (target <2s after file load).
- No direct parsing of all raw JSON in browser.
- All planned sections render with real computed values.
- At least 5 strong narrative cards generated from actual patterns.
- Prototype can be rebuilt from scratch via documented commands.

### 1.9 Risks and Mitigations (Prototype)
- Risk: noisy data leads to weak story cards.
  - Mitigation: use thresholds and confidence scoring for chapter generation.
- Risk: metric ambiguity.
  - Mitigation: define metric semantics in one place (`METRICS.md` or script constants).
- Risk: frontend bloat.
  - Mitigation: serve only compact processed payloads.

---

## 2) MVP (Cloud, Multi-User Uploads, Deploy on Vercel)

### 2.1 Objectives
- Allow anyone to upload their Spotify export and get a generated Stream Story.
- Keep UX simple: upload -> processing -> dashboard.
- Maintain ephemeral analytics model (no long-term raw data persistence unless user opts in).

### 2.2 High-Level Architecture (MVP)
- Client uploads export directly to object storage.
- App creates processing job record.
- Background worker pulls file, runs DuckDB transforms in isolated runtime (`:memory:` or `/tmp`).
- Worker writes compact output payloads to object storage.
- Frontend polls job status, then renders dashboard from processed payloads.
- Cleanup job deletes raw/processed artifacts after TTL.

### 2.3 Core Services
- Next.js app on Vercel (Node runtime routes/functions).
- Object storage for uploads + processed outputs (Vercel Blob or S3-compatible).
- Job orchestration (Vercel Queues/Workflow or equivalent).
- Metadata store (Postgres recommended) for:
  - job status
  - upload references
  - created/expiry timestamps
  - error logs

### 2.4 MVP Product Flow
1. User lands on upload page with requirements checklist.
2. Client uploads zip/json directly to storage with signed URL/token.
3. API creates `jobId` and enqueues processing.
4. Worker normalizes + computes metrics using DuckDB.
5. Worker emits `processed/{jobId}/*.json` payloads.
6. User sees progress updates and completion state.
7. Dashboard loads from processed payloads.
8. Retention policy deletes artifacts after configured TTL.

### 2.5 Multi-User Concurrency Strategy
- One isolated processing context per `jobId`.
- Never share one DuckDB file across users/jobs.
- Use idempotent workers and job locks to avoid duplicate processing.
- Enforce per-user and global concurrency limits.
- Backpressure using queue depth + rate limits.

### 2.6 Data/Privacy Posture (MVP)
- Default ephemeral retention (e.g. 24h or 7d).
- Encrypt at rest (handled by storage provider) and HTTPS in transit.
- No public object access by default.
- Signed URLs for limited-time read access.
- Clear user-facing policy:
  - what is stored
  - how long
  - delete-on-request behavior

### 2.7 External Setup Tasks You Will Need (Outside App Code)

#### A) Vercel Project and Runtime
1. Create/import Vercel project and connect Git repo.
2. Set Node runtime for server routes that use DuckDB (not Edge).
3. Configure environment variables in Vercel project settings.
4. Define production + preview env separation.

#### B) Object Storage Configuration
1. Choose storage provider (Vercel Blob or S3/R2/GCS-compatible).
2. Create private bucket/container.
3. Configure credentials/secrets in Vercel env vars.
4. Set lifecycle policy for automatic deletion (TTL).
5. Confirm max object size and multipart upload behavior.

#### C) Job/Background Processing
1. Enable queue/workflow service in Vercel account.
2. Configure worker function timeout/memory to match dataset size.
3. Set retry policy + dead-letter handling.
4. Add observability for job latency/failures.

#### D) Database for Job Metadata
1. Provision managed Postgres (Vercel Postgres/Neon/Supabase/etc.).
2. Apply schema migrations for job tables.
3. Add connection strings and pool settings.
4. Set retention cleanup for old job rows.

#### E) Security and Compliance
1. Add rate limits to upload and job creation endpoints.
2. Add abuse controls (size/type validation, malware checks if needed).
3. Draft privacy policy and terms updates for user uploads.
4. Add incident logging/alerting channel.

#### F) Domain and Ops
1. Configure custom domain and HTTPS.
2. Set monitoring dashboards (errors, job duration, queue depth).
3. Configure alerting thresholds.
4. Add scheduled cleanup cron if storage lifecycle is insufficient.

### 2.8 MVP Technical Milestones
1. Upload + job creation pipeline works end-to-end.
2. Worker produces same outputs as local prototype for reference dataset.
3. Dashboard renders from cloud payloads.
4. Concurrency test with multiple simultaneous jobs.
5. TTL cleanup verified.
6. Security hardening baseline complete.

### 2.9 MVP Acceptance Criteria
- Any user can upload supported Spotify export and receive dashboard.
- Processing completes reliably within target time budget.
- Simultaneous jobs do not interfere with each other.
- Storage artifacts expire automatically by policy.
- Critical observability in place (errors, retries, throughput).

### 2.10 Post-MVP Enhancements
- Account system + saved histories (optional, opt-in persistence).
- LLM-assisted narrative generation with confidence controls.
- Shareable story links/cards.
- Export to PDF/video recap.
- Playlist reconstruction and “memory lane” playback mode.

---

## 3) Suggested Immediate Next Step
- Implement the prototype preprocessing pipeline first (`raw -> processed payloads`) and freeze payload schema before further UI work. This de-risks both prototype speed and MVP migration.
