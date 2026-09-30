"""Infrastructure-only checks; Python standard library, no application code."""

import argparse
import json
import os
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CONNECTOR = "ledgersync-postgres-cdc"
BASE_URL = os.environ.get("CONNECT_URL", "http://connect:8083").rstrip("/")


def request_json(path, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def connector_running():
    status = request_json(f"/connectors/{CONNECTOR}/status")
    tasks = status.get("tasks", [])
    failed = status.get("connector", {}).get("state") == "FAILED" or any(
        task.get("state") == "FAILED" for task in tasks
    )
    if failed:
        raise RuntimeError("Connector/task FAILED; inspect Kafka Connect logs.")
    return (
        status.get("connector", {}).get("state") == "RUNNING"
        and len(tasks) == 1
        and all(task.get("state") == "RUNNING" for task in tasks)
    )


def wait_running():
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if connector_running():
                print(f"PASS: {CONNECTOR} and its task are RUNNING.")
                return
        except HTTPError as exc:
            if exc.code not in (404, 409, 502, 503, 504):
                raise
        except (URLError, TimeoutError):
            pass
        time.sleep(2)
    raise RuntimeError("Connector did not become RUNNING within 120 seconds.")


def configure():
    config_path = Path(__file__).with_name("connector.json")
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    if payload.get("name") != CONNECTOR:
        raise RuntimeError(f"Expected connector name {CONNECTOR} in connector.json.")
    config = payload["config"]
    config["database.password"] = os.environ["CDC_PASSWORD"]
    plugins = request_json("/connector-plugins")
    if not any(plugin.get("class") == config["connector.class"] for plugin in plugins):
        raise RuntimeError("Debezium PostgreSQL connector is not installed.")

    # Reuse an identical connector on repeated smoke tests to avoid restarts.
    current = None
    try:
        current = request_json(f"/connectors/{CONNECTOR}/config")
    except HTTPError as exc:
        if exc.code != 404:
            raise
    if current is None or any(current.get(key) != value for key, value in config.items()):
        deadline = time.monotonic() + 60
        while True:
            try:
                request_json(f"/connectors/{CONNECTOR}/config", "PUT", config)
                break
            except HTTPError as exc:
                if exc.code != 409 or time.monotonic() >= deadline:
                    raise
                time.sleep(2)
        print("Connector configuration registered (credentials not printed).")
    else:
        print("Reusing existing connector configuration.")
    wait_running()


def check_event(marker):
    # Require a streaming INSERT, not merely an old event or snapshot record.
    matches = 0
    for line_number, line in enumerate(sys.stdin, start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Malformed JSON in Kafka output at line {line_number}.") from exc
        if not isinstance(event, dict):
            continue
        event = event.get("payload", event)
        if not isinstance(event, dict):
            continue
        source = event.get("source") or {}
        after = event.get("after") or {}
        if (
            event.get("op") == "c"
            and source.get("db") == "ledger_source"
            and source.get("schema") == "public"
            and source.get("table") == "cdc_smoke"
            and after.get("marker") == marker
        ):
            matches += 1
    if matches != 1:
        raise RuntimeError(
            f"Expected exactly one streaming CDC insert for marker {marker}; found {matches}."
        )
    print(f"PASS: exactly one streaming CDC insert found for {marker}.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("configure")
    subcommands.add_parser("status")
    event_parser = subcommands.add_parser("event")
    event_parser.add_argument("marker")
    args = parser.parse_args()
    if args.command == "configure":
        configure()
    elif args.command == "status":
        wait_running()
    else:
        check_event(args.marker)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, HTTPError, URLError, TimeoutError, KeyError, ValueError) as exc:
        # Never print connector configurations or HTTP request bodies.
        print(f"FAIL: {exc}", file=sys.stderr)
        sys.exit(1)
