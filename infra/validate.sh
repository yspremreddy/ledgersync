#!/usr/bin/env bash
# Run with Bash inside WSL2, from any working directory. No host Python/jq needed.
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose --project-directory "$ROOT" -f "$ROOT/compose.yaml")
REPORT="$ROOT/.local/infra-validation/$(date -u +%Y%m%dT%H%M%S)-$$"
mkdir -p "$REPORT"
exec > >(tee "$REPORT/validation.log") 2>&1

finish() {
  local result="$1"
  set +e
  echo "== Final service status =="
  "${COMPOSE[@]}" ps -a
  local ids=()
  mapfile -t ids < <("${COMPOSE[@]}" ps -q postgres kafka connect)
  if ((${#ids[@]})); then
    echo "== Observed container memory (point-in-time, not peak) =="
    docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' "${ids[@]}" | tee "$REPORT/memory.txt"
    docker inspect --format '{{.Name}} status={{.State.Status}} OOMKilled={{.State.OOMKilled}} restarts={{.RestartCount}}' "${ids[@]}" | tee "$REPORT/container-state.txt"
  fi
  "${COMPOSE[@]}" logs --no-color --tail=150 postgres kafka connect > "$REPORT/services.log" 2>&1
  if ((result == 0)); then
    echo "PASS: infrastructure validation and CDC smoke test completed."
  else
    echo "FAIL: validation exited with status $result; no resources were deleted."
  fi
  echo "Evidence: $REPORT"
  echo "Services remain running. Stop without deleting data: docker compose down"
  exit "$result"
}
trap 'finish "$?"' EXIT

kafka_tool() {
  "${COMPOSE[@]}" exec -T -e 'KAFKA_HEAP_OPTS=-Xms16m -Xmx128m' kafka "/opt/kafka/bin/$@"
}

app_check() {
  "${COMPOSE[@]}" --profile tools run --rm --no-deps -T app python /app/check_cdc.py "$@"
}

echo "== 1. Validate Compose and build Python 3.13 image =="
"${COMPOSE[@]}" version
"${COMPOSE[@]}" --profile tools config --quiet
"${COMPOSE[@]}" --profile tools build app
"${COMPOSE[@]}" --profile tools run --rm --no-deps -T app

echo "== 2. Start PostgreSQL and Kafka =="
"${COMPOSE[@]}" up -d --wait --wait-timeout 240 postgres kafka

echo "== 3. Verify databases and logical replication =="
"${COMPOSE[@]}" exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 -c 'SELECT current_database();'
"${COMPOSE[@]}" exec -T postgres psql -U ledgersync_admin -d ledgersync -v ON_ERROR_STOP=1 -c 'SELECT current_database();'
wal_level="$("${COMPOSE[@]}" exec -T postgres psql -U ledgersync_admin -d postgres -Atqc 'SHOW wal_level')"
[[ "$wal_level" == logical ]] || { echo "Expected wal_level=logical; got $wal_level"; exit 1; }
"${COMPOSE[@]}" exec -T postgres psql -U ledgersync_admin -d ledger_source -v ON_ERROR_STOP=1 < infra/postgres/smoke.sql

echo "== 4. Provision explicit application and Connect topics =="
for topic in ledgersync.gateway.v1 ledgersync.ledger.outbox.v1 ledgersync.settlement.v1 ledgersync.refund.v1 ledgersync.dlq.v1; do
  kafka_tool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config cleanup.policy=delete --config retention.ms=86400000 --config retention.bytes=16777216
done
for topic in ledgersync.connect.configs ledgersync.connect.offsets ledgersync.connect.status; do
  kafka_tool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1 --config cleanup.policy=compact
done
kafka_tool kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
  --topic ledgersync.cdc.public.cdc_smoke --partitions 1 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=3600000 --config retention.bytes=16777216
kafka_tool kafka-topics.sh --bootstrap-server kafka:9092 --describe | tee "$REPORT/topics.txt"

echo "== 5. Start Kafka Connect and register minimal connector =="
"${COMPOSE[@]}" up -d --wait --wait-timeout 240 connect
app_check configure

# A RUNNING Connect task alone does not prove that logical streaming is active.
slot_active=f
for ((attempt=0; attempt<60; attempt++)); do
  slot_active="$("${COMPOSE[@]}" exec -T postgres psql -U ledgersync_admin -d ledger_source -Atqc \
    "SELECT active FROM pg_replication_slots WHERE slot_name='ledgersync_smoke_slot' AND database='ledger_source' AND plugin='pgoutput'")"
  [[ "$slot_active" == t ]] && break
  sleep 2
done
[[ "$slot_active" == t ]] || { echo 'Replication slot did not become active.'; exit 1; }

echo "== 6. Insert a unique smoke row =="
marker="smoke-$(date -u +%Y%m%dT%H%M%S)-${RANDOM}-${RANDOM}"
printf '%s\n'