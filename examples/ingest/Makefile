.PHONY: help up core-up monitoring-up ingest-cpp ingest-cpp-bg ingest-all benchmark-cpp-matrix down logs check-data

DOCKER_DIR := $(CURDIR)/docker
PROJECT_NAME := ingest
COMPOSE := docker compose --project-name $(PROJECT_NAME) --project-directory $(DOCKER_DIR) -f $(DOCKER_DIR)/docker-compose.yml

INGEST_SERVICES := cpp-ingest
INGEST_ARGS ?=
CPP_THREAD_VALUES ?= 1 4 16 32
CPP_FLUSH_VALUES ?= 1000 10000 50000
CPP_BUFFER_VALUES_MB ?= 50 100 500
CPP_BENCH_OUT_DIR ?= $(CURDIR)/bench-results

help:
	@echo "Make targets:"
	@echo "  up             Start Kudu cluster + monitoring stack"
	@echo "  core-up        Start Kudu master/tservers"
	@echo "  monitoring-up  Start Prometheus/Grafana/Pushgateway"
	@echo "  ingest-cpp     Run C++ ingest pipeline"
	@echo "  ingest-cpp-bg  Run C++ ingest in background (metrics)"
	@echo "  benchmark-cpp-matrix  Sweep C++ ingest tuning matrix"
	@echo "  ingest-all     Run all ingest pipelines sequentially"
	@echo "  down           Stop containers and remove volumes"
	@echo "  logs           Tail docker-compose logs"
	@echo ""
	@echo "Optional variables:"
	@echo "  INGEST_ARGS            Extra CLI flags for cpp ingest (for ingest-cpp and ingest-cpp-bg)"
	@echo "  CPP_THREAD_VALUES      Matrix thread counts (default: 1 4 16 32)"
	@echo "  CPP_FLUSH_VALUES       Matrix flush intervals (default: 1000 10000 50000)"
	@echo "  CPP_BUFFER_VALUES_MB   Matrix buffer sizes MB (default: 50 100 500)"
	@echo "  CPP_BENCH_OUT_DIR      Output directory for matrix artifacts"
	@echo "  INGEST_METRICS_PUSH_GATEWAY / INGEST_METRICS_PUSH_JOB in docker/.env control push defaults"

up: core-up monitoring-up
monitoring-up:
	$(COMPOSE) --profile monitoring up -d


core-up:
	$(COMPOSE) --profile core up -d

ingest-cpp: check-data up
	@env_file="$(DOCKER_DIR)/.env"; \
	kudu_master="$$(grep '^KUDU_MASTER=' "$$env_file" | cut -d= -f2)"; \
	metrics_port="$$(grep '^INGEST_METRICS_PORT=' "$$env_file" | cut -d= -f2)"; \
	push_gateway="$$(grep '^INGEST_METRICS_PUSH_GATEWAY=' "$$env_file" | cut -d= -f2)"; \
	push_job="$$(grep '^INGEST_METRICS_PUSH_JOB=' "$$env_file" | cut -d= -f2)"; \
	tsv_file="$$(grep '^INGEST_TSV_FILE=' "$$env_file" | cut -d= -f2)"; \
	push_args=""; \
	if [ -n "$$push_gateway" ]; then \
	  push_args="$$push_args --metrics-push-gateway $$push_gateway"; \
	fi; \
	if [ -n "$$push_job" ]; then \
	  push_args="$$push_args --metrics-push-job $$push_job"; \
	fi; \
	$(COMPOSE) --profile ingest run --build --rm --service-ports cpp-ingest \
	  /data/$$tsv_file --master $$kudu_master --metrics-port $$metrics_port $$push_args $(INGEST_ARGS)

ingest-cpp-bg: check-data up
	@docker rm -f cpp-ingest >/dev/null 2>&1 || true
	@env_file="$(DOCKER_DIR)/.env"; \
	kudu_master="$$(grep '^KUDU_MASTER=' "$$env_file" | cut -d= -f2)"; \
	metrics_port="$$(grep '^INGEST_METRICS_PORT=' "$$env_file" | cut -d= -f2)"; \
	tsv_file="$$(grep '^INGEST_TSV_FILE=' "$$env_file" | cut -d= -f2)"; \
	$(COMPOSE) --profile ingest run --build -d --name cpp-ingest --service-ports cpp-ingest \
	  /data/$$tsv_file --master $$kudu_master --metrics-port $$metrics_port $(INGEST_ARGS)

ingest-all: check-data up
	@for svc in $(INGEST_SERVICES); do \
		echo "Running ingest pipeline: $$svc"; \
		$(COMPOSE) --profile ingest up --build --abort-on-container-exit --exit-code-from $$svc --no-deps $$svc; \
	done

benchmark-cpp-matrix: check-data up
	@mkdir -p "$(CPP_BENCH_OUT_DIR)"
	@summary_csv="$(CPP_BENCH_OUT_DIR)/summary.csv"; \
	echo "run_id,threads,flush_interval,buffer_mb,rows_inserted,elapsed_seconds,rows_per_second,error_count,json_file,log_file" > "$$summary_csv"; \
	for threads in $(CPP_THREAD_VALUES); do \
	  for flush in $(CPP_FLUSH_VALUES); do \
	    for buffer in $(CPP_BUFFER_VALUES_MB); do \
	      echo "=== cpp benchmark: threads=$$threads flush=$$flush buffer_mb=$$buffer ==="; \
	      run_tag="t$${threads}_f$${flush}_b$${buffer}_$$(date +%Y%m%d%H%M%S)"; \
	      log_file="$(CPP_BENCH_OUT_DIR)/$${run_tag}.log"; \
	      json_file="$(CPP_BENCH_OUT_DIR)/$${run_tag}.json"; \
	      docker exec ingest-kudu-master-1 kudu table delete kudu-master:7051 default.hits >/dev/null 2>&1 || true; \
	      $(MAKE) ingest-cpp INGEST_ARGS="--run-id $$run_tag --num-threads $$threads --flush-interval $$flush --buffer-size-mb $$buffer" > "$$log_file" 2>&1; \
	      run_status="$$?"; \
	      cat "$$log_file"; \
	      if [ "$$run_status" -eq 0 ]; then \
	        awk '/^\{/{line=$$0} END{if (line) print line}' "$$log_file" > "$$json_file"; \
	        rows_inserted="$$(sed -n 's/.*"rows_inserted":\([0-9.]*\).*/\1/p' "$$json_file")"; \
	        elapsed_seconds="$$(sed -n 's/.*"elapsed_seconds":\([0-9.]*\).*/\1/p' "$$json_file")"; \
	        rows_per_second="$$(sed -n 's/.*"rows_per_second":\([0-9.]*\).*/\1/p' "$$json_file")"; \
	        error_count="$$(sed -n 's/.*"error_count":\([0-9.]*\).*/\1/p' "$$json_file")"; \
	        echo "$$run_tag,$$threads,$$flush,$$buffer,$$rows_inserted,$$elapsed_seconds,$$rows_per_second,$$error_count,$$json_file,$$log_file" >> "$$summary_csv"; \
	      else \
	        echo "Run failed for $$run_tag; see $$log_file" >&2; \
	        exit 1; \
	      fi; \
	    done; \
	  done; \
	done; \
	echo "Benchmark artifacts written to $(CPP_BENCH_OUT_DIR)"; \
	echo "Summary: $$summary_csv"

down:
	$(COMPOSE) --profile core --profile monitoring --profile ingest down --volumes

logs:
	$(COMPOSE) logs -f

check-data:
	@env_file="$(DOCKER_DIR)/.env"; \
	if [ ! -f "$$env_file" ]; then \
		echo "Missing $$env_file (set INGEST_DATA_DIR and INGEST_TSV_FILE)" >&2; \
		exit 1; \
	fi; \
	data_dir="$$(grep '^INGEST_DATA_DIR=' "$$env_file" | cut -d= -f2)"; \
	tsv_file="$$(grep '^INGEST_TSV_FILE=' "$$env_file" | cut -d= -f2)"; \
	if [ -z "$$data_dir" ] || [ -z "$$tsv_file" ]; then \
		echo "INGEST_DATA_DIR/INGEST_TSV_FILE not set in $$env_file" >&2; \
		exit 1; \
	fi; \
	abs_data_dir="$$(cd "$(DOCKER_DIR)" && cd "$$data_dir" 2>/dev/null && pwd)"; \
	if [ -z "$$abs_data_dir" ]; then \
		echo "Data directory not found: $$data_dir (relative to $(DOCKER_DIR))" >&2; \
		exit 1; \
	fi; \
	if [ ! -f "$$abs_data_dir/$$tsv_file" ]; then \
		echo "TSV file not found: $$abs_data_dir/$$tsv_file" >&2; \
		echo "Update $(DOCKER_DIR)/.env or place the dataset there." >&2; \
		exit 1; \
	fi
