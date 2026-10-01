#!/usr/bin/env python3
"""Repeatable HTTP load test for the quota and idempotency path.

Each worker thread reuses one keep-alive connection. Every round creates fresh tenants,
so rounds are independent; the summary reports the median of the rounds.
"""

import argparse
import base64
import concurrent.futures
import http.client
import json
import math
import os
import random
import statistics
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse


def request(parsed, method, path, body=None, headers=None):
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=15)
    payload = json.dumps(body).encode() if body is not None else None
    request_headers = dict(headers or {})
    if payload is not None:
        request_headers["Content-Type"] = "application/json"
    started = time.perf_counter()
    try:
        conn.request(method, path, body=payload, headers=request_headers)
        response = conn.getresponse()
        raw = response.read()
        elapsed_ms = (time.perf_counter() - started) * 1000
        return response.status, json.loads(raw) if raw else {}, elapsed_ms
    finally:
        conn.close()


_local = threading.local()


def keep_alive_request(parsed, method, path, body, headers):
    """Send over this thread's persistent connection, reconnecting once if the server closed it."""
    payload = json.dumps(body).encode()
    request_headers = dict(headers, **{"Content-Type": "application/json"})
    for attempt in (1, 2):
        conn = getattr(_local, "conn", None)
        if conn is None:
            conn = _local.conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=15)
        started = time.perf_counter()
        try:
            conn.request(method, path, body=payload, headers=request_headers)
        except (BrokenPipeError, ConnectionResetError, http.client.RemoteDisconnected):
            # The request never reached the server; a fresh connection is safe to use.
            conn.close()
            _local.conn = None
            if attempt == 2:
                raise
            continue
        response = conn.getresponse()
        raw = response.read()
        elapsed_ms = (time.perf_counter() - started) * 1000
        if response.will_close:
            conn.close()
            _local.conn = None
        return response.status, json.loads(raw) if raw else {}, elapsed_ms


def percentile(values, rank):
    values = sorted(values)
    return round(values[max(0, math.ceil(len(values) * rank) - 1)], 2)


def warm_up(parsed, admin_headers, count, workers):
    """Exercise the write path once so JIT compilation and pool growth do not land in round 1."""
    status, tenant, _ = request(parsed, "POST", "/admin/tenants",
                                {"name": "load-test-warmup", "quotaUnits": count}, admin_headers)
    if status != 201:
        raise RuntimeError(f"create warm-up tenant failed: HTTP {status} {tenant}")
    status, issued, _ = request(parsed, "POST", f"/admin/tenants/{tenant['id']}/keys", {}, admin_headers)
    if status != 200:
        raise RuntimeError(f"issue warm-up key failed: HTTP {status} {issued}")
    prefix = uuid.uuid4().hex[:12]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        statuses = list(pool.map(lambda i: keep_alive_request(
            parsed, "POST", "/v1/usage", {"requestId": f"warm-{prefix}-{i}", "model": "load-test", "units": 1},
            {"X-Api-Key": issued["apiKey"]})[0], range(count)))
    if any(s != 200 for s in statuses):
        raise RuntimeError("warm-up requests failed")


def run_round(parsed, admin_headers, args, number):
    tenant_ids = []
    api_keys = []
    expected_units = [len(range(i, args.unique, args.tenants)) for i in range(args.tenants)]
    for i in range(args.tenants):
        status, tenant, _ = request(parsed, "POST", "/admin/tenants",
                                    {"name": f"load-test-{i}", "quotaUnits": expected_units[i]}, admin_headers)
        if status != 201:
            raise RuntimeError(f"create tenant failed: HTTP {status} {tenant}")
        tenant_ids.append(tenant["id"])
        status, issued, _ = request(parsed, "POST", f"/admin/tenants/{tenant['id']}/keys", {}, admin_headers)
        if status != 200:
            raise RuntimeError(f"issue key failed: HTTP {status} {issued}")
        api_keys.append(issued["apiKey"])

    prefix = uuid.uuid4().hex[:12]
    ids = [(i % args.tenants, f"{prefix}-{i}") for i in range(args.unique)]
    calls = ids + [ids[i % args.unique] for i in range(args.retries)]
    random.Random(42).shuffle(calls)

    def charge(call):
        tenant_index, request_id = call
        try:
            status, result, elapsed = keep_alive_request(parsed, "POST", "/v1/usage",
                                                         {"requestId": request_id, "model": "load-test", "units": 1},
                                                         {"X-Api-Key": api_keys[tenant_index]})
            return status, result.get("replayed"), result.get("code"), elapsed
        except Exception as exc:
            return 0, None, type(exc).__name__, None

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(charge, calls))
    wall_seconds = time.perf_counter() - started

    stored_units = 0
    ledger_units = 0
    consistent = True
    per_tenant_used = []
    for i, tenant_id in enumerate(tenant_ids):
        status, account, _ = request(parsed, "GET", f"/admin/tenants/{tenant_id}", headers=admin_headers)
        if status != 200:
            raise RuntimeError(f"get tenant failed: HTTP {status} {account}")
        status, reconciliation, _ = request(parsed, "GET", f"/admin/tenants/{tenant_id}/reconciliation",
                                            headers=admin_headers)
        if status != 200:
            raise RuntimeError(f"reconciliation failed: HTTP {status} {reconciliation}")
        per_tenant_used.append(account["usedUnits"])
        stored_units += account["usedUnits"]
        ledger_units += reconciliation["ledgerUnits"]
        consistent = consistent and reconciliation["consistent"] and account["usedUnits"] == expected_units[i]

    errors = [{"status": s, "code": code} for s, _, code, _ in results if s != 200]
    latencies = [ms for _, _, _, ms in results if ms is not None]
    accepted = sum(1 for s, replayed, _, _ in results if s == 200 and replayed is False)
    replayed = sum(1 for s, replay, _, _ in results if s == 200 and replay is True)
    passed = (not errors and accepted == args.unique and replayed == args.retries
              and stored_units == args.unique and consistent and ledger_units == args.unique)
    return {
        "round": number,
        "accepted": accepted,
        "replayed": replayed,
        "errors": len(errors),
        "error_examples": errors[:5],
        "wall_seconds": round(wall_seconds, 3),
        "requests_per_second": round(len(calls) / wall_seconds, 2),
        "latency_ms": {"p50": percentile(latencies, 0.5), "p95": percentile(latencies, 0.95),
                       "p99": percentile(latencies, 0.99)} if latencies else None,
        "stored_units": stored_units,
        "ledger_units": ledger_units,
        "per_tenant_used_units": per_tenant_used,
        "reconciliation_consistent": consistent,
        "passed": passed,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--unique", type=int, default=1000)
    parser.add_argument("--retries", type=int, default=200)
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--tenants", type=int, default=1)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=2000, help="untimed requests before round 1; 0 disables")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (args.unique < 1 or args.retries < 0 or args.workers < 1 or args.rounds < 1 or args.warmup < 0
            or args.tenants < 1 or args.tenants > args.unique):
        parser.error("unique, workers and rounds must be positive; retries and warmup cannot be negative; "
                     "tenants must be 1..unique")
    parsed = urlparse(args.url)
    if parsed.scheme != "http" or not parsed.hostname:
        parser.error("--url must be an http:// URL")
    admin_user = os.environ.get("ADMIN_USERNAME", "admin")
    admin_password = os.environ.get("ADMIN_PASSWORD")
    if not admin_password:
        parser.error("set ADMIN_PASSWORD before running the load test")
    basic = base64.b64encode(f"{admin_user}:{admin_password}".encode()).decode()
    admin_headers = {"Authorization": f"Basic {basic}"}

    if args.warmup:
        warm_up(parsed, admin_headers, args.warmup, args.workers)
    rounds = [run_round(parsed, admin_headers, args, number) for number in range(1, args.rounds + 1)]
    passed = all(r["passed"] for r in rounds)
    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "loopback HTTP; application and database on same host; no upstream model",
        "client": "Python http.client, one keep-alive connection per worker thread",
        "tenants": args.tenants,
        "unique_requests": args.unique,
        "retry_requests": args.retries,
        "concurrent_clients": args.workers,
        "warmup_requests": args.warmup,
        "summary": {
            "rounds": len(rounds),
            "median_requests_per_second": round(statistics.median(r["requests_per_second"] for r in rounds), 2),
            "median_latency_ms": {k: round(statistics.median(r["latency_ms"][k] for r in rounds), 2)
                                  for k in ("p50", "p95", "p99")},
            "total_errors": sum(r["errors"] for r in rounds),
            "all_rounds_passed": passed,
        },
        "rounds": rounds,
    }
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
