#!/usr/bin/env python3
"""Send sample telemetry events to a running water-box instance."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Send sample events to water-box and print the result."
    )
    parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:8080/api/v1/events",
        help="water-box event API URL",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="number of events to send; 0 means run until interrupted",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5,
        help="seconds to wait after each event",
    )
    parser.add_argument(
        "--timeout", type=float, default=10, help="per-request timeout in seconds"
    )
    arguments = parser.parse_args()
    if arguments.count < 0 or arguments.interval <= 0 or arguments.timeout <= 0:
        parser.error("--count must be non-negative; --interval and --timeout must be positive")
    return arguments


def build_event(index):
    return {
        "eventType": "design.viewed",
        "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "userId": f"simulated-user-{index % 5}",
        "communityId": "simulated-community",
        "designId": f"simulated-design-{index % 3}",
        "payload": {
            "source": "simulate_traffic.py",
            "sequence": index,
            "referrer": "local-test",
        },
    }


def send_event(endpoint, event, timeout):
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return response.status, body
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode("utf-8")
    except urllib.error.URLError as error:
        return None, str(error.reason)


def main():
    arguments = parse_arguments()
    started_at = time.monotonic()
    successes = 0
    failures = 0
    index = 0
    print(
        f"开始发送埋点：endpoint={arguments.endpoint}，"
        f"每 {arguments.interval:g} 秒一条。按 Ctrl+C 停止。",
        flush=True,
    )

    try:
        while arguments.count == 0 or index < arguments.count:
            index += 1
            status, body = send_event(
                arguments.endpoint, build_event(index), arguments.timeout
            )
            if status == 202:
                successes += 1
            else:
                failures += 1
            print(f"[{index}] status={status} response={body}", flush=True)

            if arguments.count == 0 or index < arguments.count:
                time.sleep(arguments.interval)
    except KeyboardInterrupt:
        print("\n收到停止信号。", flush=True)

    elapsed = time.monotonic() - started_at
    print(f"完成：成功 {successes}/{index}，失败 {failures}，耗时 {elapsed:.2f}s")
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
