---
id: stale-seed-deploy-silent-decode-halt
title: >-
  A seed deploy from a stale checkout reverted the chain column to empty and
  every decode_logs model appended zero rows, all green
status: remediated
scope: "seeds (event_signatures, contracts_abi, function_signatures); every decode_logs model; any manual dbt seed run; any proxy registry (abi_source_address) that gains a new mastercopy"
symptom: >-
  Decode models stop appending (max(block_timestamp) frozen) while runs and
  refreshes complete green; a refresh window with verified raw data inserts 0
  rows; compiled SQL shows correct filters.
last_verified: 2026-09-07
evidence:
  - "prod fingerprint 2026-08-24 ~09:00 UTC: dbt.event_signatures 1,356 rows ALL chain='' , dbt.contracts_abi 180 rows ALL chain='' — exactly matches the CSVs at commit 3a210482 (the pre-rebuild fix/gpay-spender-registry tip: 1,356 / 180 rows, zero ',gnosis'); branch tip e076d2c7 CSVs are 1,432 / 199 rows all gnosis"
  - "filter locus: macros/decoding/decode_logs.sql:169 (WHERE chain = '{{ chain }}') and :184 (topic0 subquery) — empty chain values make the ABI CTE empty, decode SELECT returns 0 rows, append inserts nothing, dbt reports OK"
  - "frozen fleet measured 2026-08-24: int_execution_safes_module_events / safes_owner_events max 2026-08-23 06:27:50, zodiac_module_proxies 2026-08-23 07:09:45, gpay_delay_events 2026-08-22 22:56:35"
  - "disproof of the rival theory: compiled batch SQL (container target/run/.../int_execution_gpay_spender_events.sql, 2026-08-24 08:21) had correct window 2026-06→2026-08, watermark block_number > 35769220, and NO incremental_end_date cap — yet inserted 0 rows; WL-027 handover's DBT_MB_INCREMENTAL_END_DATE theory was wrong"
  - "guard hole: seeds/schema.yml already carried not_null + accepted_values('gnosis','celo') on chain, but scripts/run_dbt_observability.sh build_test_batches selected only tag:production model paths + sources — seeds were in no batch, so the tripwire never ran on schedule"
  - "remediation 2026-08-24 09:17 UTC: dbt seed --select event_signatures contracts_abi --full-refresh from the bind-mounted branch tip; verified 1,432 / 199 rows, 0 empty chain, 22 Spender signature rows; function_signatures was untouched by the bad deploy (4,139 rows, chain intact) so decode_calls never broke"
  - "enforcement fix in tree, pending deploy: path:seeds batch added to build_test_batches in scripts/run_dbt_observability.sh (both full and preview_subset scopes)"
  - "second shape of the same halt, 2026-09-07: contracts_gpay_modules_registry carried the post-June-2026-migration Roles mastercopy 0x732b9e9f259fba6f65a1a12dc89c20872ffbd2f (35,743 proxies, first_seen 2026-06-03) but event_signatures had RolesMod_v2 rows only for 0x9646fdad...; decode_logs resolves the ABI by abi_source_address, so every new-generation proxy decoded to nothing and int_execution_gpay_roles_events froze at 2026-06-03 10:12:10 (1,845 June rows vs ~200k/month before) while runs stayed green and the runner then refused it daily (gap > 30). Raw execution.logs at registry RolesModule addresses 2026-06-01..15: 355,029 logs, 9 topics, all 9 already seeded for the old mastercopy — the ABI is byte-identical (sha256 076465a63e04a142)"
  - "fix 2026-09-07 (seeds in tree, deployed to dbt.event_signatures/function_signatures/contracts_abi from the working tree): fetch_abi_to_csv.py 0x732b9e9f... --name RolesMod_v2 --chain gnosis, signature seeds regenerated and set-diffed vs HEAD (0 removed, +22 event / +32 function rows, chain distribution unchanged); new tripwire tests/data_quality/dq_daily_registry_abi_coverage.sql (severity error) flagged the gap before the deploy and passes after"
---
## Symptom

A decode model's backfill or daily run completes green but appends nothing; the
table's max(block_timestamp) freezes at some boundary (here 2024-08-31 23:55
for the mid-backfill model, 2026-08-23 06:27 for the daily fleet) while raw
`execution.logs` demonstrably continues. Because failure is a zero-row SELECT,
no error, no test failure, no log line distinguishes it from "no new events".

## Root cause

`decode_logs` builds its ABI CTE from `event_signatures` filtered by
`WHERE chain = 'gnosis'`. A manual `dbt seed` executed from a stale checkout
(the pre-rebuild branch tip, whose CSVs predate the populated `chain` column)
replaced `event_signatures` and `contracts_abi` wholesale with versions where
every `chain` is the empty string. From that instant every decode invocation
matched zero ABI rows and appended zero rows — including the very backfill the
seed deploy was meant to unblock (its batches 1–5 ran before the deploy and
landed 232k rows; batches 6+ ran after it and landed nothing, which mimicked a
date-cap and sent the next session down the wrong path).

## Forbidden action

Never run `dbt seed` (or any manual seed load) without first confirming the
checkout is the intended branch tip, and never walk away from a seed deploy
without verifying the loaded table against the working-tree CSV (row count and
a value-distribution spot check on the discriminator columns, `chain` above
all). A seed deploy is a full-table REPLACE — a stale checkout is a silent
mass-revert.

## Detection

- `SELECT chain, count() FROM dbt.event_signatures GROUP BY chain` — any ''
  bucket means this lesson fired (same for contracts_abi / function_signatures).
- For proxy registries: every distinct `abi_source_address` must have signature
  rows on its chain — `tests/data_quality/dq_daily_registry_abi_coverage.sql`
  (severity error) checks `contracts_gpay_modules_registry`; a registry that gains
  a mastercopy without an ABI row freezes exactly at the migration timestamp.
- Freshness sweep: `max(block_timestamp)` over decode models vs now; a shared
  freeze timestamp across independent decode models points at the shared seed,
  not at the models.
- The seed schema tests (`accepted_values` on chain) fail on '' — they run in
  CI and, once the `path:seeds` batch deploys, in every scheduled test run.

## Safe remediation

Re-run `dbt seed --select event_signatures contracts_abi function_signatures
--full-refresh` from the correct checkout (the dbt container bind-mounts the
working tree), verify counts + chain distribution, then let watermark-based
incrementals catch up on their own (`--incremental-only` refresh for a
mid-backfill model; the daily runner for the fleet). For a missing mastercopy
ABI: `python scripts/signatures/fetch_abi_to_csv.py 0xMASTERCOPY --name <Name>
--chain gnosis`, regenerate the signature seeds, SET-diff them against HEAD
(zero removed rows), seed, then catch the frozen decode up in month-sized
`incremental_end_date` chunks — its block watermark makes each chunk append-only. No partitions were wiped —
the failure mode is pure non-append, so no history recovery is needed beyond
re-running the silent window.

## Ground truth

Raw `execution.logs` for the affected contracts (address IN registry, bounded
block_timestamp window) versus the decoded table's per-month counts. The raw
side is untouched by this failure class.

## Enforcement

`path:seeds` test batch in `scripts/run_dbt_observability.sh` (in tree, pending
deploy) makes the existing seed `chain` tests run on schedule; flip this lesson
to `enforced` once that batch has demonstrably failed-or-passed in a production
run.
