#!/usr/bin/env python3
"""Send sample telemetry events to a running water-box instance."""

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        "--count", type=int, default=10, help="number of events to send"
    )
    parser.add_argument(
        "--concurrency", type=int, default=1, help="parallel request count"
    )
    parser.add_argument(
        "--timeout", type=float, default=10, help="per-request timeout in seconds"
    )
    arguments = parser.parse_args()
    if arguments.count < 1 or arguments.concurrency < 1 or arguments.timeout <= 0:
        parser.error("--count, --concurrency, and --timeout must be positive")
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
    failures = []

    with ThreadPoolExecutor(max_workers=arguments.concurrency) as executor:
        futures = [
            executor.submit(
                send_event, arguments.endpoint, build_event(index), arguments.timeout
            )
            for index in range(arguments.count)
        ]
        for index, future in enumerate(as_completed(futures), start=1):
            status, body = future.result()
            if status == 202:
                successes += 1
            else:
                failures.append((status, body))
            print(f"[{index}/{arguments.count}] status={status} response={body}")

    elapsed = time.monotonic() - started_at
    print(
        f"\n完成：成功 {successes}/{arguments.count}，"
        f"失败 {len(failures)}，耗时 {elapsed:.2f}s"
    )
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
