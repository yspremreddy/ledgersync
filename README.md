# LedgerSync

Minimal infrastructure foundation for a transaction reconciliation platform.
This phase contains **no reconciliation, analytics, or FastAPI functionality**.

## Requirements

- Windows with WSL2 and Docker Desktop using Linux containers; enable Docker integration for your WSL distribution.
- Docker Compose v2.24 or later and Bash inside WSL2.
- Approximately 3.5 GB allocated to Docker. Stop unrelated containers first.
- Internet access for the initial image pulls/build. No PaySim download, host Python, Java, PostgreSQL, curl, or jq installation is required.
- Keep the checkout in the WSL Linux filesystem, such as `~/projects/ledgersync`, rather than `/mnt/c` where practical.
- Host ports 15432, 19092, and 18083 must be available.

## Run and validate

From the repository root in WSL2:

```bash
bash infra/validate.sh
```

The script:

1. Checks shell syntax and Compose configuration and builds the Python 3.13 image.
2. Starts PostgreSQL and single-node KRaft Kafka and waits for health checks.
3. Verifies both initialized databases and `wal_level=logical`.
4. Creates a tiny source table/publication and explicitly provisions topics.
5. Starts Kafka Connect, checks for the PostgreSQL plugin, registers the connector, and waits for its task and replication slot.
6. Inserts one uniquely marked row into `ledger_source.public.cdc_smoke`.
7. Consumes Kafka and requires a matching Debezium streaming insert (`op=c`), not a stale event or snapshot.
8. Rechecks service health, records OOM/restart state, and measures current container memory.

The consumer deliberately waits for a 30-second idle timeout before checking its output. A timeout in `consumer.log` alone is not a failure; the script must still find the newly inserted marker.

Each run writes evidence beneath `.local/infra-validation/<timestamp>-<pid>/`: validation output, topic descriptions, the marker, Kafka events, service logs, container state, and memory measurements. This directory is ignored by Git. Connector configuration and credentials are not printed by the helper.

**Validation status at authoring:** configuration has been reviewed through repository tools only. Docker/Compose execution was unavailable. Image pulls, startup, CDC delivery, and actual memory use remain unverified until the script runs successfully on a Docker host. Configured ceilings are not measurements.

The script leaves services running and preserves all data. Rerunning it reuses the databases, topics, connector, and slot, and inserts one new test row. Run one validation at a time. It creates missing topics but does not silently change existing topic configurations.

## Services and resource budget

| Service | Image | Memory ceiling | Address from WSL host |
| --- | --- | --- | --- |
| PostgreSQL | `postgres:16.8-bookworm` | 448 MiB | `localhost:15432` |
| Kafka broker/controller | `apache/kafka:3.9.1` | 768 MiB | `localhost:19092` |
| Debezium Connect | `quay.io/debezium/connect:3.0.0.Final` | 640 MiB | `http://localhost:18083` |
| Python tooling, one-shot | `python:3.13.2-slim-bookworm` | 128 MiB | None |
| Kafka volume ownership initialization, one-shot | Same Kafka image | 64 MiB | None |

The three steady-service ceilings total **1,856 MiB**. Kafka's heap is capped at 384 MiB and Connect's at 256 MiB, leaving non-heap headroom. Kafka CLI commands run sequentially with separate small heaps inside the Kafka container. The script captures point-in-time memory after processing; it does not establish peak usage or a sustained-load guarantee. Docker VM overhead and filesystem cache need additional headroom.

The versions are pinned release tags, not immutable digests or a claim that they are the latest releases. Runtime compatibility and registry availability must be confirmed by validation before upgrading or promoting this foundation.

Only three services remain running. No ZooKeeper or application server is included. The Python image is non-root, standard-library-only, and runs validation helpers; it is not a placeholder API process.

## PostgreSQL and CDC

Fresh PostgreSQL volume initialization creates:

- `ledger_source`: source database for the smoke test.
- `ledgersync`: empty database reserved for the future platform.
- `ledger_cdc`: dedicated login/replication role with SELECT access only to the smoke table when validation provisions it.

The connector uses `pgoutput`, publication `ledgersync_smoke_publication`, and persistent slot `ledgersync_smoke_slot`. The administrator creates the publication; connector publication auto-creation is disabled. `snapshot.mode=no_data` means the test verifies new streamed changes, not a historical data snapshot.

Connect configuration/status/offset state lives in Kafka compacted internal topics and is preserved in the Kafka named volume. PostgreSQL tables and replication slots live in its named volume. Connect therefore needs no independent state volume.

The smoke connector captures a test table directly. The eventual ledger outbox and its application logic are intentionally **not implemented**.

PostgreSQL limits retained slot WAL to 256 MB at checkpoints. This is not an instantaneous disk limit. A sufficiently long outage can invalidate the slot; that is a recovery condition, not a reason to automatically delete data. This script reports failure rather than dropping slots or clearing offsets.

## Topics

Provisioned application topics, each with three partitions and replication factor one:

- `ledgersync.gateway.v1`
- `ledgersync.ledger.outbox.v1`
- `ledgersync.settlement.v1`
- `ledgersync.refund.v1`
- `ledgersync.dlq.v1`

The actual smoke event reaches **`ledgersync.cdc.public.cdc_smoke`**, with one partition. The reserved ledger outbox topic remains empty in this phase.

Connect internal topics each have one partition, replication factor one, and compaction:

- `ledgersync.connect.configs`
- `ledgersync.connect.offsets`
- `ledgersync.connect.status`

Source topics use bounded retention, and automatic broker topic creation is disabled. Kafka's consumer-offset internal topic is managed by Kafka. Single-node replication is suitable for this local demonstration only and supplies no failover.

## Configuration and local boundaries

Defaults are local-development credentials: administrator `ledgersync_admin` / `local-postgres-only`, and CDC user `ledger_cdc` / `local-cdc-only`. Optional `POSTGRES_PASSWORD` and `CDC_PASSWORD` values can be placed in a repository-local `.env` before the first run; `.env` is ignored by Git and the image build.

Database initialization scripts run only on an empty PostgreSQL volume. Changing `.env` later does not rotate existing database passwords. Keep existing values for repeat runs unless deliberately rotating the corresponding database role as well.

Published ports bind to loopback. Kafka and Connect are unauthenticated local infrastructure and must not be exposed publicly. Container clients use `kafka:9092`; host clients use `localhost:19092`. Do not use the host listener from other containers.

Compose owns two project-scoped named volumes, `postgres_data` and `kafka_data`. The short-lived ownership service changes only the root directory ownership of this project's Kafka volume. No Docker socket is mounted into a container. No script prunes Docker, deletes volumes, or modifies files outside this checkout.

## Inspect and stop

```bash
# Validate configuration without creating containers.
docker compose --profile tools config --quiet

# Inspect status and logs.
docker compose ps -a
docker compose logs --tail=100 postgres kafka connect

# Run only the minimal Python image after building it.
docker compose --profile tools run --rm --no-deps app

# Stop and remove this project's containers/network; preserve named volumes.
docker compose down
```

No destructive reset is part of the validation workflow. On failure, inspect the generated evidence, fix the reported configuration or environment issue, and rerun validation. An occupied host port, failed image pull, invalidated replication slot, or insufficient Docker memory must not be reported as a passing smoke test.
