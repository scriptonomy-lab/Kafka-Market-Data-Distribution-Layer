# =============================================================================
# Makefile — every command you need, with a one-line description each.
# Run `make` (or `make help`) to list them.
#
# Conventions
#   * Services run in Docker (docker-compose.yml): one container per service,
#     Storage scalable. `make up` starts everything.
#   * Host-side tools (experiments, preflight, tests) run from the project
#     virtualenv (.venv) and reach Kafka at localhost:9092 and TimescaleDB at
#     localhost:$POSTGRES_HOST_PORT (default 5432).
#   * Extra arguments go through ARGS, e.g.  make experiment-f ARGS="--step 60"
# =============================================================================

PY       ?= .venv/bin/python
COMPOSE  ?= docker compose
KAFKA_BIN = /opt/kafka/bin
KAFKA     = $(COMPOSE) exec -T kafka $(KAFKA_BIN)
ARGS     ?=
N        ?= 2
SVC      ?=
# Every optional profile, so `down` and `reset` also remove ui/dev/tools containers
ALL_PROFILES = --profile ui --profile dev --profile tools

.DEFAULT_GOAL := help
.PHONY: help setup build up up-infra up-dev down reset scale ps logs \
        watch-pricing watch-risk watch-monitor demo ui \
        topics create-topics describe-groups tail-topic partition-map assignment-check \
        db-init psql preflight capture seed-book reconcile \
        experiment-a experiment-b experiment-c experiment-f replay charts fanout-baseline \
        test test-integration test-report lint fmt evidence-bundle

help: ## List all targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- environment
setup: ## Create .venv (host tools), .env from the template, and the DB password
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	@test -f .env || (cp .env.example .env && echo "Created .env - now add your two TraderMade keys")
	@# Random database password, never typed or printed. Mounted into the
	@# database and Storage containers as a Compose secret.
	@mkdir -p secrets && test -f secrets/db_password || (umask 077 && openssl rand -hex 24 > secrets/db_password && echo "Created secrets/db_password")

dirs:
	@mkdir -p logs run results/monitor

# --------------------------------------------------------------------- stack
build: ## Build the one application image (tradermade-kafka:v2)
	$(COMPOSE) build producer

up: dirs ## Start the full stack on the LIVE feed (storage x2)
	$(COMPOSE) up -d --build
	@$(COMPOSE) ps

up-infra: dirs ## Start only Kafka + TimescaleDB and create topics and schema
	$(COMPOSE) up -d --build kafka-init db-init

up-dev: dirs ## DEV ONLY: full stack on the offline simulator (results go to results/_dev)
	@# The simulator gets a dummy key: the real one never leaves for a fake server.
	TRADERMADE_WS_URL=ws://fake-feed:8765/feedAdv TRADERMADE_WS_KEY=dev-simulator-key \
		$(COMPOSE) --profile dev up -d --build
	@$(COMPOSE) ps

down: ## Stop and remove containers (Kafka and TimescaleDB data are kept)
	$(COMPOSE) $(ALL_PROFILES) down

reset: ## DANGER: delete Kafka + TimescaleDB volumes, logs and run state (clean slate)
	$(COMPOSE) $(ALL_PROFILES) down -v
	rm -rf logs run results/monitor

scale: ## Run N Storage instances, leaving the others untouched: make scale N=3
	$(COMPOSE) up -d --scale storage=$(N) --no-recreate storage

ps: ## Containers, their state and health
	$(COMPOSE) $(ALL_PROFILES) ps -a

logs: ## Follow container logs: make logs SVC=storage (all services if empty)
	$(COMPOSE) logs -f --tail 50 $(SVC)

watch-pricing: ## Attach to the Pricing table (detach: Ctrl-p Ctrl-q, NOT Ctrl-c)
	docker attach --sig-proxy=false tm-pricing

watch-risk: ## Attach to the Risk table (detach: Ctrl-p Ctrl-q)
	docker attach --sig-proxy=false tm-risk

watch-monitor: ## Attach to the Monitoring panel (detach: Ctrl-p Ctrl-q)
	docker attach --sig-proxy=false tm-monitor

demo: ## tmux layout of the running containers, for screenshots
	bash scripts/tmux_demo.sh

ui: ## Optional Kafbat UI on http://localhost:8080
	$(COMPOSE) --profile ui up -d kafka-ui

# ---------------------------------------------------------------------- kafka
topics: ## Describe the three topics: partitions, leaders, retention
	$(KAFKA)/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic 'marketdata\..*'

create-topics: ## (Re)run the kafka-init job: create topics from topology.toml (idempotent)
	$(COMPOSE) run --rm kafka-init

describe-groups: ## Kafka's view of every group: offsets/lag, then members and assignments
	$(KAFKA)/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
	$(KAFKA)/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups --members --verbose

tail-topic: ## Print the next 20 events (all topics) with key, partition and offset; gives up after 15 s
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
		--include 'marketdata\..*' --max-messages 20 --timeout-ms 15000 \
		--property print.key=true --property print.partition=true \
		--property print.offset=true --property print.timestamp=true

partition-map: ## Symbol -> partition map (ARGS=--scan to compare counts, --check vs live)
	$(PY) -m scripts.partition_map $(ARGS)

assignment-check: ## Does every Storage instance own data with 2/3 instances? (real broker)
	$(PY) -m scripts.assignment_check $(ARGS)

# ------------------------------------------------------------------- database
db-init: ## (Re)run the db-init job: apply db/migrations (idempotent)
	$(COMPOSE) run --rm db-init

psql: ## Interactive psql in the TimescaleDB container
	$(COMPOSE) exec timescaledb psql -U tradermade -d tradermade

# -------------------------------------------------------------------- helpers
preflight: ## PASS/FAIL readiness: containers, topics, DB, symbols, market hours
	$(PY) -m scripts.preflight $(ARGS)

capture: ## Raw TraderMade frames + sub_ack report; producer must be STOPPED (one session per key)
	$(PY) -m scripts.capture_raw $(ARGS)

seed-book: ## Seed the risk book from REST /live (one-shot container with the REST key), restart risk
	$(COMPOSE) --profile tools run --rm seed-book
	$(COMPOSE) restart risk

reconcile: ## Compare Pricing's latest state with REST /live
	$(PY) -m scripts.reconcile $(ARGS)

# ---------------------------------------------------------------- experiments
experiment-a: ## A: stop ALL Storage instances cleanly (docker stop), then restart
	$(PY) -m experiments.failure_recovery --mode stop $(ARGS)

experiment-b: ## B: crash ALL Storage instances (SIGKILL), outage, restart N
	$(PY) -m experiments.failure_recovery --mode crash $(ARGS)

experiment-c: ## C: slow Storage down, then restore it
	$(PY) -m experiments.failure_recovery --mode slow $(ARGS)

experiment-f: ## F: scale-out and rebalance: 2 -> 3 -> clean stop -> crash
	$(PY) -m experiments.scale_rebalance $(ARGS)

replay: ## D: a brand-new group replays all three topics; compare with TimescaleDB
	$(PY) -m experiments.replay $(ARGS)

charts: ## Redraw charts for a run: make charts ARGS=results/<run-id>
	$(PY) -m experiments.make_charts $(ARGS)

fanout-baseline: ## Stretch E: direct fan-out without Kafka (stop the producer container first)
	$(PY) -m experiments.direct_fanout $(ARGS)

# ---------------------------------------------------------------------- tests
test: ## Unit tests (no Kafka needed)
	$(PY) -m pytest -q

test-integration: ## Integration tests against Kafka + TimescaleDB from docker compose
	$(PY) -m pytest -q -m integration

test-report: ## Save unit + integration test output to results/<stamp>-tests/
	bash scripts/test_report.sh

lint: ## Lint and format check
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

fmt: ## Auto-format and auto-fix lint issues
	$(PY) -m ruff format .
	$(PY) -m ruff check --fix .

# ------------------------------------------------------------------ deliverables
evidence-bundle: ## dist/evidence-<date>.zip: README, docs/evidence, v2 results, tests, safe logs
	$(PY) -m scripts.evidence_bundle $(ARGS)
