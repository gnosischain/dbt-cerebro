#!/usr/bin/env python3
"""Propose question_synonyms for approved metrics that have fewer than 3.

Reads target/semantic_registry.json (no dbt needed) and, for every APPROVED
metric with < --min synonyms, generates candidates from:

  (a) label variants — lowercased, de-pluralized, word-order tweaks;
  (b) description noun phrases (first sentence, stopwords stripped);
  (c) module vocabulary (domain phrasings a user would actually type).

GLOBAL UNIQUENESS is enforced: a candidate already used by ANY metric (or
already proposed for another metric this run) is dropped — duplicate synonyms
create 90-point discovery ties in the cerebro-mcp scorer.

Output: one markdown review table per module (metric | current | proposed) to
--out-dir (default: the scratchpad-friendly ./target/synonym_review/). A human
reviews and pastes accepted synonyms into semantic/authoring/**; nothing is
written to authoring files by this script.

Usage:
    python scripts/semantic/build_registry.py --target-dir target   # fresh registry
    python scripts/semantic/suggest_synonyms.py [--min 3] [--modules gpay,consensus]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import defaultdict

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

STOPWORDS = {
    "the", "a", "an", "of", "for", "and", "or", "per", "by", "in", "on", "to",
    "daily", "weekly", "monthly", "latest", "value", "count", "total", "with",
    "auto", "generated", "candidate", "metric", "review", "promote", "before",
    "relying", "it", "this", "is", "are", "as", "read", "do", "not", "sum",
}

MODULE_VOCAB = {
    "consensus": ["validators", "staking", "beacon chain"],
    "gpay": ["gnosis pay", "card payments"],
    "gnosis_app": ["gnosis app", "app.gnosis.io"],
    "gnosis_app_gt": ["gnosis app ground truth"],
    "Circles": ["circles", "circles avatars"],
    "revenue": ["dao revenue", "protocol fees"],
    "bridges": ["bridges", "cross-chain"],
    "p2p": ["p2p network", "node network"],
    "ESG": ["sustainability", "energy"],
    "pools": ["dex pools", "liquidity"],
    "yields": ["yield", "apy opportunities"],
    "lending": ["aave", "lending"],
    "tokens": ["token balances", "token supply"],
    "transactions": ["transactions", "tx activity"],
    "transfers": ["token transfers"],
    "safe": ["gnosis safe", "smart wallets"],
    "mixpanel_ga": ["web analytics", "app analytics"],
    "probelab": ["network probes"],
    "quarterly_data": ["quarterly report"],
}


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", text.lower())).strip()


def _depluralize(word: str) -> str:
    if len(word) > 3 and word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def candidates_for(metric: dict, module: str) -> list[str]:
    out: list[str] = []
    label = _norm(metric.get("label") or "")
    if label:
        out.append(label)
        words = label.split()
        deplural = " ".join(_depluralize(w) for w in words)
        if deplural != label:
            out.append(deplural)
        content = [w for w in words if w not in STOPWORDS]
        if 1 < len(content) < len(words):
            out.append(" ".join(content))
    desc = (metric.get("description") or "").split(".")[0]
    desc_words = [w for w in _norm(desc).split() if w not in STOPWORDS]
    if 2 <= len(desc_words) <= 6:
        out.append(" ".join(desc_words))
    base = out[0] if out else _norm(metric.get("name", "").replace("_", " "))
    for vocab in MODULE_VOCAB.get(module, [])[:2]:
        if vocab not in base:
            out.append(f"{vocab} {base}".strip()[:60])
    seen: set[str] = set()
    uniq = []
    for c in out:
        c = c.strip()
        if c and c not in seen and len(c) >= 4:
            seen.add(c)
            uniq.append(c)
    return uniq


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default=str(REPO_ROOT / "target" / "semantic_registry.json"))
    ap.add_argument("--min", type=int, default=3)
    ap.add_argument("--modules", default="", help="comma-separated module filter")
    ap.add_argument("--out-dir", default=str(REPO_ROOT / "target" / "synonym_review"))
    args = ap.parse_args()

    registry_path = pathlib.Path(args.registry)
    if not registry_path.exists():
        print(f"ERROR: registry not found at {registry_path}. Run build_registry.py first.")
        return 2
    registry = json.loads(registry_path.read_text())
    metrics = registry.get("metrics", {})
    module_filter = {m.strip() for m in args.modules.split(",") if m.strip()}

    taken: set[str] = set()
    for metric in metrics.values():
        for syn in (metric.get("semantic", {}).get("meta", {}) or {}).get(
            "question_synonyms"
        ) or metric.get("question_synonyms") or []:
            taken.add(_norm(syn))

    by_module: dict[str, list[tuple[str, list[str], list[str]]]] = defaultdict(list)
    proposed_global: set[str] = set()
    for name, metric in sorted(metrics.items()):
        if metric.get("quality_tier") != "approved":
            continue
        module = metric.get("module", "") or "unknown"
        if module_filter and module not in module_filter:
            continue
        current = (metric.get("semantic", {}).get("meta", {}) or {}).get(
            "question_synonyms"
        ) or metric.get("question_synonyms") or []
        if len(current) >= args.min:
            continue
        needed = args.min - len(current)
        fresh = [
            c for c in candidates_for(metric, module)
            if _norm(c) not in taken and _norm(c) not in proposed_global
        ][: needed + 2]
        for c in fresh:
            proposed_global.add(_norm(c))
        if fresh:
            by_module[module].append((name, current, fresh))

    out_dir = pathlib.Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    total = 0
    for module, rows in sorted(by_module.items()):
        path = out_dir / f"{module.replace('/', '_')}.md"
        lines = [
            f"# Synonym proposals — {module}",
            "",
            "Review each row; paste accepted synonyms into the metric's",
            "`question_synonyms` in semantic/authoring/. Proposals are globally",
            "unique across all metrics (duplicates create discovery ties).",
            "",
            "| metric | current | proposed |",
            "|---|---|---|",
        ]
        for name, current, fresh in rows:
            lines.append(
                f"| `{name}` | {', '.join(current) or '—'} | {', '.join(fresh)} |"
            )
        path.write_text("\n".join(lines) + "\n")
        total += len(rows)
        print(f"{module:24} {len(rows):4} metrics -> {path.relative_to(REPO_ROOT)}")
    print(f"\n{total} approved metrics below the {args.min}-synonym target.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
