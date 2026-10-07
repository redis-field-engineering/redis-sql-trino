#!/usr/bin/env python3
"""Read Redis Cloud API mode and task evidence without changing the deployment."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import urllib.error
import urllib.request
from uuid import UUID


DATABASE_FIELDS = (
    "databaseId", "name", "status", "redisVersion", "respVersion",
    "supportOSSClusterApi", "useExternalEndpointForOSSClusterApi",
    "memoryLimitInGb", "datasetSizeInGb", "replication",
    "dataPersistence", "dataEvictionPolicy",
)


def mode_matches(database, client_mode):
    # Missing flags and non-boolean values cannot establish the server mode.
    return (database.get("status") == "active"
            and database.get("supportOSSClusterApi") is (client_mode == "oss"))


def task_evidence(task):
    response = task.get("response") or {}
    error = response.get("error") or {}
    return {
        **{key: task[key] for key in ("taskId", "status", "timestamp") if key in task},
        "error": {key: error[key] for key in ("type", "status", "description") if key in error},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("subscription", type=int)
    parser.add_argument("database", type=int)
    parser.add_argument("--client-mode", choices=("default", "oss"), required=True)
    parser.add_argument("--task", type=UUID, action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    account = os.environ.get("REDIS_CLOUD_API_KEY")
    user = os.environ.get("REDIS_CLOUD_API_SECRET_KEY")
    if not account or not user:
        parser.error("Set REDIS_CLOUD_API_KEY and REDIS_CLOUD_API_SECRET_KEY")

    def get(path):
        request = urllib.request.Request("https://api.redislabs.com/v1" + path, headers={
            "x-api-key": account, "x-api-secret-key": user,
            "Accept": "application/json", "Content-Type": "application/json",
            "User-Agent": "redis-sql-trino-api-diagnostic",
        }, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # Never print a raw API response or authenticated request.
            raise RuntimeError(f"GET {path}: HTTP {error.code}") from None
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise RuntimeError(f"GET {path}: request failed or invalid JSON") from None

    try:
        database = get(f"/subscriptions/{args.subscription}/databases/{args.database}")
        tasks = [task_evidence(get(f"/tasks/{task}")) for task in args.task]
    except RuntimeError as error:
        print(error, file=sys.stderr)
        return 2
    matches = mode_matches(database, args.client_mode)
    evidence = {
        "checkedAt": datetime.now(timezone.utc).isoformat(),
        "subscriptionId": args.subscription,
        "database": {key: database[key] for key in DATABASE_FIELDS if key in database},
        "clientMode": args.client_mode,
        "serverClientModeMatches": matches,
        "tasks": tasks,
    }
    # Also remove credential values if a service error happens to echo one.
    output = json.dumps(evidence, indent=2).replace(account, "[redacted]").replace(user, "[redacted]") + "\n"
    if args.output:
        args.output.write_text(output)
    print(output, end="")
    return 0 if matches else 1


if __name__ == "__main__":
    sys.exit(main())
