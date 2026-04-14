# Ingest Docker Compose

This Compose setup runs a local Kudu cluster, the C++ TSV ingest example, and an
observability stack (Prometheus, Grafana, and Pushgateway). The Kudu services
expose Prometheus metrics at `/metrics_prometheus`.

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
2. Start the Kudu services and monitoring stack:

```bash
cd examples/ingest/docker
docker compose --profile core --profile monitoring up -d
```

3. Run the C++ ingest container (builds the image on first run):

```bash
docker compose --profile ingest up --build
```

Prometheus UI is available at `http://localhost:${PROMETHEUS_PORT}`.
Grafana is available at `http://localhost:${GRAFANA_PORT}` (default login
`admin` / `admin`). The ingest metrics endpoint is available at
`http://localhost:${INGEST_METRICS_PORT}/metrics`, and Pushgateway is available
at `http://localhost:${PUSHGATEWAY_PORT}`.

Grafana dashboards are pre-provisioned under the **Kudu** folder:
- `kudu-overview` — cluster health, ingest throughput, tablet distribution
- `kudu-write-path` — WAL and flush/compaction latency breakdowns

## Tablet servers

The Compose file starts three tablet servers by default. Their web UIs are
available on `http://localhost:8050`, `http://localhost:8052`, and
`http://localhost:8053`. To change the count, edit `docker-compose.yml` and add
or remove tserver services.

## Environment variables

The `.env` file controls image versions, data paths, and ports:

- `KUDU_IMAGE`: pinned Kudu image tag for master/tserver containers.
- `PROMETHEUS_IMAGE`: pinned Prometheus image tag.
- `PROMETHEUS_PORT`: host port for Prometheus (default: 9090).
- `PROMETHEUS_RETENTION`: Prometheus TSDB retention window (default: 12h).
- `PUSHGATEWAY_IMAGE`: Pushgateway image tag.
- `PUSHGATEWAY_PORT`: host port for Pushgateway (default: 9091).
- `GRAFANA_IMAGE`: Grafana image tag.
- `GRAFANA_PORT`: host port for Grafana (default: 3000).
- `GRAFANA_ADMIN_USER`: Grafana admin username.
- `GRAFANA_ADMIN_PASSWORD`: Grafana admin password.
- `KUDU_MASTER`: master RPC address used by the C++ ingest container.
- `INGEST_DATA_DIR`: host directory containing TSV files.
- `INGEST_TSV_FILE`: TSV filename within `INGEST_DATA_DIR`.
- `INGEST_METRICS_PORT`: port for the C++ ingest `/metrics` endpoint.
- `INGEST_METRICS_PUSH_GATEWAY`: Pushgateway endpoint used by batch ingest runs.
- `INGEST_METRICS_PUSH_JOB`: Pushgateway job label for ingest metrics.
- `INGEST_METRICS_OUTPUT`: optional JSON metrics output path (inside the ingest container).
- `INGEST_METRICS_KEEPALIVE_SECONDS`: keep metrics endpoint alive after ingest.

The ingest container mounts `INGEST_DATA_DIR` read-write so it can write a
metrics summary file (for example, set `INGEST_METRICS_OUTPUT=/data/ingest_metrics.json`).
