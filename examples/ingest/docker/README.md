# Ingest Docker Compose

This Compose setup runs a local Kudu cluster and the C++ TSV ingest example.

## Prerequisites

- Docker Desktop (or Docker Engine + Compose plugin)
- A TSV file available on the host (see `../README.md` for dataset links)

## Quick start

1. Update the `.env` file to point at your TSV file and data directory.
   The Compose file enables `--unlock_unsafe_flags` via `MASTER_ARGS` and
   `TSERVER_ARGS` because the Kudu 1.18.1 images default to
   `--use_hybrid_clock=false`, which requires explicit opt-in.
   Kudu containers run as `root` so they can write to the named volumes
   under `/var/lib/kudu`.
2. Start the Kudu services:

```bash
cd examples/ingest/docker
docker compose --profile core up -d
```

3. Run the C++ ingest container (builds the image on first run):

```bash
docker compose --profile ingest up --build
```

The ingest metrics endpoint is available at
`http://localhost:${INGEST_METRICS_PORT}/metrics`.

## Tablet servers

The Compose file starts three tablet servers by default. Their web UIs are
available on `http://localhost:8050`, `http://localhost:8052`, and
`http://localhost:8053`. To change the count, edit `docker-compose.yml` and add
or remove tserver services.

## Environment variables

The `.env` file controls image versions, data paths, and ports:

- `KUDU_IMAGE`: pinned Kudu image tag for master/tserver containers.
- `KUDU_MASTER`: master RPC address used by the C++ ingest container.
- `INGEST_DATA_DIR`: host directory containing TSV files.
- `INGEST_TSV_FILE`: TSV filename within `INGEST_DATA_DIR`.
- `INGEST_METRICS_PORT`: port for the C++ ingest `/metrics` endpoint.
- `INGEST_METRICS_OUTPUT`: optional JSON metrics output path (inside the ingest container).
- `INGEST_METRICS_KEEPALIVE_SECONDS`: keep metrics endpoint alive after ingest.

The ingest container mounts `INGEST_DATA_DIR` read-write so it can write a
metrics summary file (for example, set `INGEST_METRICS_OUTPUT=/data/ingest_metrics.json`).
