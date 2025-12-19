# sentra (React + C# refresh)

## Layout
- `collector/agent.py` - headless sampler that writes host + GPU data into MySQL.
- `sentra_api/` - ASP.NET Core API surfacing `/api/telemetry/summary`, `/api/telemetry/history`, `/api/telemetry/purge`.
- `web/` - Vite + React + TypeScript dashboard that hits the C# API.
- `docker/Dockerfile.agent` - Python collector image.
- `docker/Dockerfile.api` - .NET publish image.
- `docker/Dockerfile.web` - builds and serves the React bundle (nginx proxies `/api` to sentra-api).
- `docker-compose.yml` - runs agent + API + web plus the `sentra-mysql` service; `sentra_data` acts as a shared log volume.

## API shape
- `GET /api/telemetry/summary` -> latest CPU, memory, GPU, disk, network, and fan samples.
- `GET /api/telemetry/history?minutes=60` -> host trend + GPU time series.
- `POST /api/telemetry/purge` with `{ "cutoffEpoch": <unix_ts> }` -> deletes rows older than cutoff.
- `GET /api/health` -> status + resolved MySQL summary.

## Dev tips
- Configure `SENTRA_DB_HOST`, `SENTRA_DB_PORT`, `SENTRA_DB_USER`, `SENTRA_DB_PASSWORD`, and `SENTRA_DB_NAME` for your MySQL server (or use `SENTRA_DB_URL`).
- `VITE_API_BASE_URL` controls which API the UI calls (default `http://localhost:5099`).
- Sampling cadence is `SENTRA_SAMPLE_INTERVAL` seconds (default 2s).
