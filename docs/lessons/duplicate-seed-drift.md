---
id: duplicate-seed-drift
title: Duplicate seed rows in an append chain carry a constant error forward every day
status: remediated
scope: >-
  cumulative append models that seed each day from the prior day's rows
  (canonical: the token balances chain)
symptom: a constant offset in a cumulative series dating back years; per-day deltas all correct
last_verified: 2026-07-17
evidence:
  - docs/data-quality-learnings-and-remediation.md (L4 — ~430 WxDAI negatives since 2023 from one duplicate (date, token, address) row read non-deterministically by any(balance_raw))
  - scripts/full_refresh/refresh.py (batch-1 --full-refresh recreate is the clean-slate lever)
---

## Symptom
A cumulative model is wrong by a constant amount for specific keys, with an origin far
in the past; every recent day's math checks out.

## Root cause
An append-strategy rebuild left a duplicate `(date, key)` row. The daily seed reads the
prior day with a non-deterministic aggregate (`any(...)`), so the duplicate injects an
offset once — and the cumulative carry-forward propagates it every day after.

## Forbidden action
Don't patch the offset in place (a compensating row just adds a second anomaly), and
don't re-run forward increments hoping it converges — carry-forward never forgets.

## Detection
Duplicate-key excess on the seed grain: `count() - uniqExact(key)` per (date, key)
grouping > 0. Origin-date hunting: walk the cumulative series backwards to the first
date the offset appears; the duplicate lives there.

## Safe remediation
Clean recreate only: the full-refresh orchestrator's batch-1 `--full-refresh` drops and
rebuilds the table, then the staged batches append non-overlapping history.

## Ground truth
On-chain `balanceOf` at the origin date's block for the affected address.

## Enforcement
Duplicate-key data-quality test (tests/data_quality/); non-negative-balance test
catches the downstream symptom.
