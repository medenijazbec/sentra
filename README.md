# sentra

sentra is a lightweight observability node that combines a Python collector, a .NET API, and a Vite/React dashboard to deliver GPU/host telemetry without Streamlit bloat. Bring-your-own SQLite volume, mount `/sys`, and flip GPUs on—sentra does the rest.

## What sentra offers

- **GPU + host sampling loop** – `collector/agent.py` grabs CPU, memory, disks, network, fans, and NVIDIA GPU stats via NVML; data lands in SQLite so it survives restarts.
- **Docker-first packaging** – three Dockerfiles (`agent`, `api`, `web`) plus a compose file make it easy to run the collector, ASP.NET Core API, and nginx-served UI on a single host.
- **Typed REST API** – `sentra_api` exposes `/api/telemetry/summary`, `/api/telemetry/history`, `/api/telemetry/purge`, and `/api/health` so other services or dashboards can reuse the data.
- **Modern dashboard** – the `web` package is a Vite + React + TypeScript app that mirrors the Honeybadger glass aesthetic: slim sparklines, GPU mini-trends, sticky nav, and quick theme switching.
- **GPU awareness** – docker-compose includes `deploy.resources.reservations.devices` for NVIDIA GPUs, and the collector handles NVML library discovery with helpful hints if host passthrough is missing.

## Quick start

```bash
cd /srv/share/sentra
docker compose build --no-cache
docker compose up -d
```

Then browse to `http://localhost:8501` (React UI) and hit `http://localhost:5099/api/telemetry/summary` for JSON. The compose stack mounts:

- `sentra_data` volume to `/data` for the SQLite DB.
- `/sys` and `/var/run/docker.sock` (read-only) for sensor + container insights.
- `--gpus all` via compose device reservations so NVML can see cards.

## Directory map

| Path | Description |
|------|-------------|
| `collector/` | Sampling loop + helpers (NVML, sensors, docker info). |
| `sentra_api/` | ASP.NET Core API exposing summary/history/purge endpoints. |
| `web/` | Vite + React UI with phosphor/night themes. |
| `docker/` | Dockerfiles + nginx config for each service. |
| `config/` | Python config helpers (DB path, sample interval). |
| `api/` | SQLite datastore wrapper used by the collector + API. |

## Local dev tips

- Set `SENTRA_DB_PATH` to place the SQLite DB somewhere else (defaults to `/data/sentra.db`).
- Want the UI to call a remote API? Set `VITE_API_BASE_URL` when building `web` or via `.env`.
- The collector respects `SENTRA_SAMPLE_INTERVAL`, `SENTRA_DOCKER_STATS`, and `SENTRA_HOST_SYS` to tune cadence and mounts.
- Use `npm run dev` inside `web/` to preview the dashboard; `dotnet watch run` inside `sentra_api/` for API changes.

## Support / contributions

Issues and PRs are welcome—open one if you hit NVML edge cases, need another collector, or want more dashboard widgets. Pull requests should include a short description and screenshots when touching the UI. |
