#!/usr/bin/env python3
"""Capture Q5 scan counters while Trino still retains the query, including failed queries."""

import argparse
import datetime
import json
import urllib.parse
import urllib.request

Q5 = "SELECT COUNT(DISTINCT UserID) FROM hits;"
STATS = (
    "elapsedTime", "executionTime", "totalCpuTime", "physicalInputPositions",
    "physicalInputReadTime", "physicalInputDataSize", "peakUserMemoryReservation",
)


def capture(info):
    stats = info.get("queryStats", {})
    scans = [
        {
            "stageId": operator.get("stageId"),
            "operatorType": operator.get("operatorType"),
            "physicalInputPositions": operator.get("physicalInputPositions"),
            "connectorMetrics": operator["connectorMetrics"],
        }
        for operator in stats.get("operatorSummaries", [])
        if operator.get("connectorMetrics")
    ]
    return {
        "queryId": info["queryId"],
        "state": info["state"],
        "queryStats": {key: stats.get(key) for key in STATS},
        "scanMetrics": scans,
        "error": info.get("errorCode"),
        "failure": failure_summary(info.get("failureInfo")),
        "successfulElapsedTime": stats.get("elapsedTime") if info["state"] == "FINISHED" else None,
    }


def failure_summary(failure):
    if not failure:
        return None
    # Keep original server errors and their cause chain, without duplicating large Java stacks.
    return {
        "type": failure.get("type"),
        "message": failure.get("message"),
        "cause": failure_summary(failure.get("cause")),
    }


def normalized(sql):
    return " ".join(sql.strip().rstrip(";").upper().split())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", default="http://127.0.0.1:18080")
    parser.add_argument("--user", default="clickbench")
    parser.add_argument("--sql", default=Q5, help="Exact SQL to select; EXPLAIN is excluded")
    parser.add_argument("--query-id", help="Capture a specific query instead of looking up matching SQL")
    args = parser.parse_args()
    base = args.server.rstrip("/") + "/v1/query"

    def get(url):
        request = urllib.request.Request(url, headers={"X-Trino-User": args.user})
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)

    ids = [args.query_id] if args.query_id else [
        query["queryId"] for query in get(base)
        if normalized(query.get("query", "")) == normalized(args.sql)
    ]
    snapshots = [capture(get(base + "/" + urllib.parse.quote(query_id, safe=""))) for query_id in ids]
    print(json.dumps({
        "capturedUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "queries": snapshots,
    }, indent=2))
    if not snapshots:
        raise SystemExit("No matching queries retained by Trino; capture before the next server restart")


if __name__ == "__main__":
    main()
