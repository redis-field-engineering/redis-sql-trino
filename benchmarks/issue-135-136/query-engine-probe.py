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


def probe(client, command):
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
    args = parser.parse_args()
    if bool(args.index) == bool(args.command_file):
        parser.error("Specify exactly one of --index or --command-file")
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
        result = probe(client, command)
    password = options.get("password")
    encoded = json.dumps({"capturedUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          "redisPyVersion": redis.__version__, "protocol": args.protocol, "probe": result}, indent=2)
    print(encoded.replace(password, "[redacted]") if password else encoded)
    raise SystemExit(1 if result["error"] else 0)


if __name__ == "__main__":
    main()
