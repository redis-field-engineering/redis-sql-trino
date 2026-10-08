#!/usr/bin/env python3
"""Replay captured FT.AGGREGATE tokens on a fresh direct connection, without retries."""
import argparse
import datetime
import json
import time
from pathlib import Path


def validate(command):
    if (not isinstance(command, list) or len(command) < 3
            or not all(isinstance(token, str) for token in command)
            or command[0].upper() != "FT.AGGREGATE"):
        raise ValueError("Command must be a JSON string array beginning with FT.AGGREGATE and an index/query")
    return command


def count_documents(reply):
    """Validate the complete global COUNT reply, including RESP3 partial-result warnings."""
    if isinstance(reply, dict):
        if reply.get("warning"):
            raise ValueError("Query Engine returned a warning: " + str(reply["warning"]))
        rows = reply.get("results", [])
        if len(rows) != 1:
            raise ValueError("Expected one global COUNT result")
        value = rows[0].get("extra_attributes", {}).get("documents")
    elif isinstance(reply, list) and len(reply) == 2 and reply[0] == 1:
        fields = reply[1]
        if not isinstance(fields, list) or len(fields) != 2 or fields[0] != "documents":
            raise ValueError("Unexpected RESP2 global COUNT fields")
        value = fields[1]
    else:
        raise ValueError("Unexpected global COUNT reply")
    if not (type(value) is int and value >= 0
            or isinstance(value, str) and value.isascii() and value.isdigit()):
        raise ValueError("Expected an exact nonnegative document count")
    return int(value)


def probe(client, command, expected_count=None):
    start = time.perf_counter()
    result = {"command": validate(command), "successfulSeconds": None, "cursorDeleted": False}
    try:
        result["reply"] = client.execute_command(*command)
        result["requestSeconds"] = time.perf_counter() - start
        # This is a command diagnostic, not a benchmark or a complete SQL result. A returned cursor is
        # deleted on the same connection; never reissue a read that could have consumed an unseen batch.
        if "WITHCURSOR" in [token.upper() for token in command[3:]]:
            reply = result["reply"]
            if not isinstance(reply, list) or len(reply) != 2 or not isinstance(reply[1], int):
                raise ValueError("Unexpected cursor response; inspect the retained reply")
            cursor = reply[1]
            result["complete"] = cursor == 0
            if cursor:
                client.execute_command("FT.CURSOR", "DEL", command[1], cursor)
                result["cursorDeleted"] = True
        else:
            result["complete"] = True
        payload = result["reply"][0] if "WITHCURSOR" in [token.upper() for token in command[3:]] else result["reply"]
        if isinstance(payload, dict) and payload.get("warning"):
            raise ValueError("Query Engine returned a warning: " + str(payload["warning"]))
        if expected_count is not None:
            result["documents"] = count_documents(result["reply"])
            if result["documents"] != expected_count:
                raise ValueError(f"Global COUNT {result['documents']} does not match expected {expected_count}")
        result["error"] = None
    except Exception as error:
        result["complete"] = False
        result["error"] = {"type": type(error).__name__, "message": str(error)}
    result["wallSeconds"] = time.perf_counter() - start
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--connection", required=True, type=Path, help="Private JSON Redis connection options")
    parser.add_argument("--index", help="Index for a lightweight global COUNT health probe")
    parser.add_argument("--command-file", type=Path, help="Exact JSON command array captured from DEBUG logs")
    parser.add_argument("--protocol", type=int, choices=(2, 3), default=3)
    parser.add_argument("--expected-count", type=int, help="Require a complete global COUNT matching the loaded dataset")
    args = parser.parse_args()
    if bool(args.index) == bool(args.command_file):
        parser.error("Specify exactly one of --index or --command-file")
    if args.expected_count is not None and (not args.index or args.expected_count < 0):
        parser.error("--expected-count requires --index and a nonnegative count")
    import redis
    options = json.loads(args.connection.read_text())
    # Redis (rather than RedisCluster) deliberately tests the selected coordinator without client routing.
    options.update(protocol=args.protocol, decode_responses=True, socket_connect_timeout=10, socket_timeout=1200)
    options["retry"] = redis.retry.Retry(redis.backoff.NoBackoff(), 0)
    options["retry_on_timeout"] = False
    command = json.loads(args.command_file.read_text()) if args.command_file else [
        "FT.AGGREGATE", args.index, "*", "TIMEOUT", "1200000", "GROUPBY", "0",
        "REDUCE", "COUNT", "0", "AS", "documents", "DIALECT", "2",
    ]
    validate(command)
    with redis.Redis(**options) as client:
        result = probe(client, command, args.expected_count)
    password = options.get("password")
    encoded = json.dumps({"capturedUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          "redisPyVersion": redis.__version__, "protocol": args.protocol, "probe": result}, indent=2)
    print(encoded.replace(password, "[redacted]") if password else encoded)
    raise SystemExit(1 if result["error"] else 0)


if __name__ == "__main__":
    main()
