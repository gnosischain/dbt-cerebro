#!/usr/bin/env python3
"""
One-time backfill of Circles v2 avatar IPFS metadata.

Reads unresolved (avatar, metadata_digest) pairs from
`int_execution_circles_v2_avatar_metadata_targets`, fetches each
JSON payload from a list of public IPFS gateways with concurrency,
retries, and gateway fallback, and inserts the results into
`circles_avatar_metadata_raw` (one row per pair, including failures).

After this completes, the nightly
`fetch_and_insert_circles_metadata` dbt run-operation handles
ongoing deltas.

Usage:
    python scripts/circles/backfill_avatar_metadata.py
    python scripts/circles/backfill_avatar_metadata.py --limit 100 --dry-run
    python scripts/circles/backfill_avatar_metadata.py --concurrency 30 --batch-size 5000

Dependencies (already in requirements.txt):
    clickhouse-connect, python-dotenv

Required env vars (from .env):
    CLICKHOUSE_URL CLICKHOUSE_PORT CLICKHOUSE_USER CLICKHOUSE_PASSWORD
    CLICKHOUSE_DATABASE CLICKHOUSE_SECURE
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

try:
    import clickhouse_connect
    from dotenv import load_dotenv
except ImportError as exc:
    print(
        "Error: missing dependency. Install with: "
        "pip install clickhouse-connect python-dotenv"
    )
    raise SystemExit(1) from exc


# ----------------------------------------------------------------------
# Setup
# ----------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("backfill_avatar_metadata")

DEFAULT_GATEWAYS: Sequence[str] = (
    "https://ipfs.io/ipfs/",
    "https://cloudflare-ipfs.com/ipfs/",
    "https://dweb.link/ipfs/",
)

USER_AGENT = "gnosis-dbt-circles-metadata-backfill/1.0"


# ----------------------------------------------------------------------
# ClickHouse client
# ----------------------------------------------------------------------

def make_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_URL", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "dbt"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "True").lower() == "true",
    )


def fetch_unresolved(client, limit: int | None) -> list[tuple[str, str, str, str]]:
    sql = """
        SELECT
            t.avatar,
            t.metadata_digest,
            t.ipfs_cid_v0,
            t.gateway_url
        FROM int_execution_circles_v2_avatar_metadata_targets t
        LEFT ANTI JOIN circles_avatar_metadata_raw r
          ON t.avatar = r.avatar
         AND t.metadata_digest = r.metadata_digest
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"

    logger.info("Querying unresolved targets%s", f" (limit={limit})" if limit else "")
    rows = client.query(sql).result_rows or []
    logger.info("Found %d unresolved targets", len(rows))
    return [tuple(row) for row in rows]


# ----------------------------------------------------------------------
# Fetcher (stdlib urllib + threadpool)
# ----------------------------------------------------------------------

def _http_get(url: str, timeout: float) -> tuple[int, str, str, str]:
    """
    Returns (status_code, content_type, body, error). status_code=0
    means a transport error (connection refused, DNS, timeout).
    """
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return (
                resp.status,
                resp.headers.get("content-type", ""),
                body,
                "",
            )
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            body = ""
        return (exc.code, exc.headers.get("content-type", "") if exc.headers else "", body, f"HTTP {exc.code}")
    except urllib.error.URLError as exc:
        return (0, "", "", f"URLError: {exc.reason}")
    except TimeoutError as exc:
        return (0, "", "", f"Timeout: {exc}")
    except Exception as exc:  # noqa: BLE001
        return (0, "", "", f"{type(exc).__name__}: {exc}")


def fetch_one(
    gateways: Sequence[str],
    avatar: str,
    metadata_digest: str,
    ipfs_cid_v0: str,
    primary_gateway_url: str,
    max_retries: int,
    request_timeout: float,
) -> tuple:
    """
    Returns the row to insert into circles_avatar_metadata_raw:
        (avatar, metadata_digest, ipfs_cid_v0, gateway_url,
         http_status, content_type, body, error, fetched_at)
    """
    fetched_at = datetime.now(tz=timezone.utc)

    # Build the gateway sequence: configured primary first, then the
    # remaining defaults (deduplicated by prefix).
    primary_prefix = primary_gateway_url[: -len(ipfs_cid_v0)] if primary_gateway_url.endswith(ipfs_cid_v0) else ""
    sequence: list[str] = []
    seen_prefix: set[str] = set()
    for gw in (primary_prefix, *gateways):
        if gw and gw not in seen_prefix:
            sequence.append(gw)
            seen_prefix.add(gw)

    last_status = 0
    last_error = ""
    used_url = primary_gateway_url

    for gw_prefix in sequence:
        url = f"{gw_prefix}{ipfs_cid_v0}"
        used_url = url
        for attempt in range(1, max_retries + 1):
            status, content_type, body, error = _http_get(url, request_timeout)
            last_status = status
            last_error = error

            if status == 200 and body:
                return (
                    avatar,
                    metadata_digest,
                    ipfs_cid_v0,
                    url,
                    status,
                    content_type,
                    body,
                    "",
                    fetched_at,
                )

            if status in (429, 500, 502, 503, 504) or status == 0:
                # Transient: retry on the same gateway with backoff
                time.sleep(min(2 ** attempt, 10))
                continue

            # Non-retryable status (e.g. 404). Move to next gateway.
            break

    return (
        avatar,
        metadata_digest,
        ipfs_cid_v0,
        used_url,
        last_status,
        "",
        "",
        last_error or "exhausted gateways",
        fetched_at,
    )


# ----------------------------------------------------------------------
# Inserter
# ----------------------------------------------------------------------

INSERT_COLUMNS = [
    "avatar",
    "metadata_digest",
    "ipfs_cid_v0",
    "gateway_url",
    "http_status",
    "content_type",
    "body",
    "error",
    "fetched_at",
]


def insert_batch(client, rows: Sequence[tuple]) -> None:
    if not rows:
        return
    client.insert(
        "circles_avatar_metadata_raw",
        list(rows),
        column_names=INSERT_COLUMNS,
    )


def chunked(seq: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def run(args: argparse.Namespace) -> int:
    client = make_client()
    targets = fetch_unresolved(client, args.limit)
    if not targets:
        logger.info("Nothing to backfill.")
        return 0

    if args.dry_run:
        logger.info("--dry-run set: would fetch %d targets", len(targets))
        for row in targets[:5]:
            logger.info("  sample: %s", row)
        return 0

    pending: list[tuple] = []
    ok = 0
    fail = 0
    total = len(targets)

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = {
            pool.submit(
                fetch_one,
                DEFAULT_GATEWAYS,
                avatar,
                metadata_digest,
                ipfs_cid_v0,
                gateway_url,
                args.max_retries,
                args.request_timeout,
            ): (avatar, metadata_digest)
            for (avatar, metadata_digest, ipfs_cid_v0, gateway_url) in targets
        }

        for i, future in enumerate(as_completed(futures), start=1):
            try:
                row = future.result()
            except Exception as exc:  # noqa: BLE001
                avatar, metadata_digest = futures[future]
                logger.warning("fetch_one crashed for %s/%s: %s", avatar, metadata_digest, exc)
                row = (
                    avatar,
                    metadata_digest,
                    "",
                    "",
                    0,
                    "",
                    "",
                    f"crash: {type(exc).__name__}: {exc}",
                    datetime.now(tz=timezone.utc),
                )

            pending.append(row)
            if row[4] == 200 and row[6]:
                ok += 1
            else:
                fail += 1

            if len(pending) >= args.batch_size:
                logger.info(
                    "Inserting batch of %d (progress: %d/%d, %d ok / %d fail)",
                    len(pending), i, total, ok, fail,
                )
                insert_batch(client, pending)
                pending.clear()

    if pending:
        logger.info("Inserting final batch of %d", len(pending))
        insert_batch(client, pending)

    logger.info("Backfill complete: %d ok, %d failed (of %d)", ok, fail, total)
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--limit", type=int, default=None, help="Cap the number of targets to fetch.")
    p.add_argument("--concurrency", type=int, default=30, help="Worker threads for HTTP fetches.")
    p.add_argument("--batch-size", type=int, default=5000, help="Rows per ClickHouse insert batch.")
    p.add_argument("--max-retries", type=int, default=3, help="Retries per gateway on transient errors.")
    p.add_argument("--request-timeout", type=float, default=20.0, help="Per-request HTTP timeout in seconds.")
    p.add_argument("--dry-run", action="store_true", help="Query unresolved targets but do not fetch or insert.")
    return p.parse_args()


def main() -> int:
    return run(parse_args())


if __name__ == "__main__":
    sys.exit(main())
