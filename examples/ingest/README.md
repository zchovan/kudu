# Kudu Data Ingestion Examples

This directory contains the C++ native ingest pipeline for loading the
ClickHouse `hits` TSV dataset into Apache Kudu, plus a Docker-based
observability stack for Prometheus and Grafana.

## Recommended Workflow

Use the `examples/ingest` Makefile with Docker Compose. It orchestrates:

- Kudu master + tservers
- Prometheus + Grafana + Pushgateway
- C++ ingest container (`cpp/kudu_insert`)

## Quick Start

1. Update `examples/ingest/docker/.env` to point at your dataset.
2. Run:

```bash
cd examples/ingest
make up
make ingest-cpp
```

3. Open:
   - Prometheus: `http://localhost:${PROMETHEUS_PORT}`
   - Grafana: `http://localhost:${GRAFANA_PORT}`
   - Pushgateway: `http://localhost:${PUSHGATEWAY_PORT}`

Batch runs (`make ingest-cpp`, `make ingest-all`, and matrix benchmarks) push
final metrics to Pushgateway by default. `make ingest-cpp-bg` keeps the ingest
process running and exposes `/metrics` for live scrape/debug sessions.

## Common Make Targets

```bash
cd examples/ingest
make up                   # Start Kudu + monitoring stack
make ingest-cpp           # One-shot ingest run (pushes metrics)
make ingest-cpp-bg        # Background ingest for live /metrics scraping
make benchmark-cpp-matrix # Parameter sweep benchmark
make down                 # Stop and remove containers/volumes
```

## Project Layout

- `cpp/` - C++ native ingest client, build files, and detailed usage docs.
- `docker/` - Compose stack, Prometheus/Grafana provisioning, environment
  defaults.
- `Makefile` - High-level orchestration commands for local runs.

For ingest flags, schema details, and native build instructions, see
`cpp/README.md`.

## Data Format

The ingest client expects TSV input with:

- No header row
- Exactly 105 columns per row
- Tab (`\t`) delimiter
- Column order matching the ClickHouse hits schema

See `cpp/README.md` for the full schema and exact column order.

## Getting Sample Data

Download the ClickHouse-compatible hits dataset and build smaller subsets for
local iteration:

```bash
wget -O hits.tsv.gz \
  "https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz" \
  --show-progress

zcat hits.tsv.gz | head -n 100000 > hits_100k.tsv
zcat hits.tsv.gz | head -n 1000000 > hits_1M.tsv
zcat hits.tsv.gz | head -n 10000000 > hits_10M.tsv
```

Place the chosen TSV under `examples/ingest/data/` (or update `INGEST_DATA_DIR`
and `INGEST_TSV_FILE` in `examples/ingest/docker/.env`).

