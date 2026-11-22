#!/usr/bin/env python3
"""Validate retained cohorts and emit every sample; no fastest-run selection."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FIELDS = ["deployment", "workload", "mode", "attempt", "rows", "elapsedNanos",
          "redisRowsReceived", "aggregateRequests", "cursorRequests", "resultSha256"]


def main():
    rows = []
    for deployment in ("non_sharded", "sharded"):
        cohort = json.loads((ROOT / "software" / deployment / "assessment.json").read_text())
        assert cohort["attemptsPerMode"] == 3
        assert cohort["factRows"] == 32 and cohort["dimensionRows"] == 6
        assert cohort["immutableDuringMeasuredQueries"] and not cohort["dynamicFiltering"]
        samples = cohort["samples"]
        assert len(samples) == 96
        workloads = {sample["workload"] for sample in samples}
        assert len(workloads) == 16
        identities = {(s["workload"], s["mode"], s["attempt"]) for s in samples}
        assert identities == {(w, m, a) for w in workloads for m in ("enabled", "disabled") for a in (1, 2, 3)}
        for sample in samples:
            assert sample["validatedAgainstValues"]
            assert sample["resultSha256"] == sample["oracleSha256"]
            metrics = sample["connectorMetrics"]
            rows.append(dict(deployment=deployment, workload=sample["workload"], mode=sample["mode"],
                             attempt=sample["attempt"], rows=sample["rows"], elapsedNanos=sample["elapsedNanos"],
                             redisRowsReceived=metrics.get("redis.rows.received", 0),
                             aggregateRequests=metrics.get("redis.aggregate.requests", 0),
                             cursorRequests=metrics.get("redis.cursor.requests", 0), resultSha256=sample["resultSha256"]))
    with (ROOT / "summary.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Validated and summarized {len(rows)} measured samples")


if __name__ == "__main__":
    main()
