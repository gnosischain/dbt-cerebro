#!/usr/bin/env python3
"""One-shot migration helper: flip incremental models to the new write policy.

Class A (append + microbatch tag): decode_logs/decode_calls models, Circles
event intermediates, live/low-latency tables, raw event streams.
Class B (insert_overwrite): everything else incremental that declares partition_by.
FLAG (manual): no partition_by and not class A; or class A with a custom
overlap filter that needs a hand-written block_number watermark.

Run with --apply to write changes; default is dry-run.
"""
from __future__ import annotations

import argparse
import glob
import re
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODELS = ROOT / "models"

# Genuine non-decode append streams with custom (non-macro) watermark logic.
# These are append-only and high-velocity; they need a hand-written
# block_number watermark, so we defer them to manual per-model edits rather
# than flip them automatically. Everything else that uses
# apply_monthly_incremental_filter (incl. Circles event + daily models) is
# class B: insert_overwrite makes the lookback overlap harmless.
A_PATH_HINTS = (
    "execution/live/",
)
A_NAME_HINTS = (
    "_events_live.sql",
)

STRAT_RE = re.compile(r"incremental_strategy\s*=\s*('(?:[^']*)'|\([^\n]*\))")
TAGS_RE = re.compile(r"tags\s*=\s*\[([^\]]*)\]")


def classify(path: str, src: str) -> str:
    head = src[:2000]
    is_decode = "decode_logs(" in src or "decode_calls(" in src
    is_a = (
        is_decode
        or any(h in path for h in A_PATH_HINTS)
        or any(path.endswith(n) or n in path for n in A_NAME_HINTS)
    )
    has_partition = "partition_by" in head
    if is_decode:
        # Macro watermark (block_number > max) makes these no-overlap append
        # regardless of slicing -> safe to auto-flip.
        return "A_DECODE"
    if is_a:
        # Non-decode append streams still rely on the microbatch runner's
        # no-overlap slice (meta annotation) or a hand-written watermark.
        # Don't auto-flip; handle per-model.
        return "A_MANUAL"
    if has_partition:
        return "B"
    return "FLAG"


def ensure_microbatch_tag(head: str) -> str:
    m = TAGS_RE.search(head)
    if not m:
        return head
    inner = m.group(1)
    if "microbatch" in inner:
        return head
    new_inner = inner.rstrip()
    if new_inner and not new_inner.endswith(","):
        new_inner += ","
    new_inner += " 'microbatch'"
    return head[: m.start(1)] + new_inner + head[m.end(1) :]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    counts = {"A_DECODE": 0, "B": 0, "A_MANUAL": 0, "FLAG": 0, "skip": 0}
    deferred = []  # unique-ish model names for the allowlist
    for f in sorted(glob.glob(str(MODELS / "**" / "*.sql"), recursive=True)):
        src = open(f, encoding="utf-8", errors="ignore").read()
        head = src[:2000]
        if not re.search(r"materialized\s*=\s*['\"]incremental['\"]", head):
            continue
        rel = f.split("models/", 1)[1]
        cls = classify(rel, src)
        m = STRAT_RE.search(head)
        cur = m.group(1) if m else "(inherit)"
        name = pathlib.Path(rel).stem

        new_head = head
        if cls == "A_DECODE":
            target = "incremental_strategy='append'"
            if m:
                new_head = new_head[: m.start()] + target + new_head[m.end():]
            new_head = ensure_microbatch_tag(new_head)
        elif cls == "B":
            target = "incremental_strategy='insert_overwrite'"
            if m:
                new_head = new_head[: m.start()] + target + new_head[m.end():]
        else:  # A_MANUAL or FLAG -> defer, add to allowlist
            counts[cls] += 1
            deferred.append(name)
            print(f"  {cls}  {rel}  (deferred: manual per-model)")
            continue

        counts[cls] += 1
        if new_head != head:
            print(f"  {cls}  {rel}  [{cur} -> {target}]")
            if args.apply:
                open(f, "w", encoding="utf-8").write(new_head + src[2000:])
        else:
            counts["skip"] += 1

    print(
        f"\nA_DECODE={counts['A_DECODE']} B={counts['B']} "
        f"A_MANUAL={counts['A_MANUAL']} FLAG={counts['FLAG']} unchanged={counts['skip']}"
    )
    if deferred and args.apply:
        allow = ROOT / "scripts" / "checks" / "no_delete_insert.allow"
        existing = set()
        if allow.exists():
            existing = {l.split("#")[0].strip() for l in allow.read_text().splitlines() if l.split("#")[0].strip()}
        # allowlist matches manifest unique_ids; we don't know the project name
        # here, so write model names and let the operator expand. Simpler: write
        # the unique_id form model.gnosis_dbt.<name>.
        lines = sorted(existing | {f"model.gnosis_dbt.{n}" for n in deferred})
        header = "# Not-yet-migrated incremental models (Wave 2/4). Shrink per wave; delete when empty.\n"
        allow.write_text(header + "\n".join(lines) + "\n")
        print(f"\nWrote {len(deferred)} deferred models to {allow}")
    print("\n(dry-run)" if not args.apply else "\n(applied)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
