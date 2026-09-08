---
id: delete-insert-unbounded-delete-timeout
title: An unbounded delete+insert DELETE on a wide table finishes server-side but dbt times out and the INSERT never runs
status: observed
scope: any delete+insert model whose target spans many month partitions and whose
  daily delete-set is a tuple-IN over the new keys with no date bound (the
  dbt-clickhouse default); bites hardest on cumulative tables in the 100M+ row class
symptom: dbt reports "The read operation timed out" after exactly the profile's
  send_receive_timeout (3000s) on a model that normally finishes in minutes; the
  table's max(date) stops advancing while its upstream is current; system.mutations
  shows the run's lightweight delete is_done=1 long before dbt gave up
last_verified: 2026-09-07
evidence:
  - 'elementary.dbt_run_results int_execution_tokens_balances_native_daily: error 2026-09-05T11:50 (3431s) and 2026-09-07T10:16:23 (3231s) "ClickHouse exception: The read operation timed out"; 2026-09-06 two Code 241 (total) overcommit kills; last success 2026-09-04 (282s)'
  - 'system.query_log 2026-09-07: 09:26:23 insert into ...__dbt_new_data (228s, 1,194,393 rows = 09-04..09-06); 09:26:23→09:33:53 delete from dbt.int_execution_tokens_balances_native_daily where (date, token_address, address) in (select ... from __dbt_new_data) QueryFinish 450s exception_code 0; NO subsequent insert into the target; dbt error time 10:16:23 = 09:26:23 + 3000s'
  - 'system.mutations dbt.int_execution_tokens_balances_native_daily mutation 0000000084 create_time 2026-09-07 09:26:23, is_done=1 on both replicas, parts_to_do=0'
  - 'same-day contrast: insert into int_execution_gnosis_app_gpay_wallets__dbt_new_data ran 2794s and succeeded (clickhouse-connect sends send_progress_in_http_headers=1 on every request, so a streaming INSERT keeps the connection alive; a mutation wait emits no progress)'
  - 'downstream impact: 38 physical + 81 api_ production models served 2026-09-03 as latest through 2026-09-07; fct_execution_tokens_metrics_daily rows for 09-04..09-06 had holders=0 and supply=0 while volume was populated'
  - 'system.part_log 2026-09-07: the unbounded delete (09:26) produced 367 MutatePart events (= every active part, 0 rows changed, ~904s part-time, 450s wall); the bounded delete after the fix (15:01, mutation 0000000085) still produced 367 MutatePart events (0 rows changed) but ~360s part-time / 243s wall under the same saturation — the bound removes the per-part scan, not the per-part version clone; the table has 369 active parts over 75 month partitions'
  - 'fix in tree, pending deploy: models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql config incremental_predicates (date >= addDays(today(), -7) on the daily path; explicit month bounds when start_month/end_month are set); dbt-clickhouse 1.9.1 appends predicates to the delete+insert DELETE only (dbt/include/clickhouse/macros/materializations/incremental/incremental.sql clickhouse__incremental_delete_insert)'
---

## Symptom
A daily delete+insert model that usually completes in a few minutes errors with
"The read operation timed out" after exactly `send_receive_timeout` (3000s in
profiles.yml). Its `max(date)` freezes while the upstream keeps advancing. Elementary
shows the failure as a timeout, not as a query error, and the cron's retry
classifier treats it as permanent, so nothing retries it. Every cumulative consumer
freezes on the same day (the token chain served 2026-09-03 for four days).

## Root cause
dbt-clickhouse's delete+insert issues
`delete from <target> where (<unique_key>) in (select <unique_key> from <new_data>)`.
That predicate carries no date bound, so the lightweight-delete mutation must evaluate
every part of the table (six years of month partitions for the token balances table).
Under server saturation the mutation took 450s. On ClickHouse Cloud (SharedMergeTree) a
lightweight delete additionally creates a new version of EVERY active part of the table,
even parts the predicate cannot touch (~1s per part here, 369 parts), so the delete's
floor is proportional to the part count, not to the rows matched. During a mutation wait the server sends
no HTTP progress headers, so the idle connection was dropped by an intermediary; the
server finished the delete at 09:33:53 but the client never received the response and
waited the full 3000s. Because the materialization is sequential, the follow-up
`insert into <target> select ... from <new_data>` never ran. The delete matched zero
rows this time (the new days did not exist yet), so no data was lost — on an
overlapping window it would have deleted and never re-inserted
(see [wide-delete-insert-wipe](wide-delete-insert-wipe.md)).

## Forbidden action
Do not raise `send_receive_timeout` or retry blindly — the server already finished;
the wait is for a response that will never arrive. Do not leave a delete+insert on a
wide table with the adapter's default unbounded delete-set.

## Detection
When a delete+insert model reports a timeout: correlate `system.query_log` (the
`delete from ...` QueryFinish time) with the dbt error time; a gap equal to
`send_receive_timeout` measured from the delete's start means the response was lost,
not the query. Confirm in `system.mutations` (`is_done=1`) and check that no
`insert into <target> select ... from ...__dbt_new_data` followed.
Per-model freshness: `max(date)` vs the direct upstream's `max(date)`.

## Safe remediation
Bound the delete with `incremental_predicates` so the mutation prunes by the partition
key: on the daily path a `date >= addDays(today(), -N)` predicate (the model's
calendar starts at `max(date WHERE date < yesterday()) + 1`, so only yesterday can
overlap existing rows); on a windowed reprocess the explicit
`toStartOfMonth(date) BETWEEN start_month AND end_month`. Then re-run the model
plainly — the daily path recomputes the missing days from the last good seed — and
let downstream self-heal (microbatch models resume from their data watermark).
The bound halves the cost but does not remove the per-part clone: keep the part count
of such tables low (merge old month partitions down to one part), or move the daily
path off mutations entirely (a mutation-free strategy) if the delete stays in the
minutes range.

## Ground truth
`system.query_log` / `system.mutations` for the run's invocation, Elementary
`dbt_run_results.message`, and the table's `max(date)` against its upstream.

## Enforcement
None yet. Candidate: extend `scripts/checks/no_delete_insert.py` so a grandfathered
delete+insert model whose target is partitioned must declare `incremental_predicates`
on its partition column.
