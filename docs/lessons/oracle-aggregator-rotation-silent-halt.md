---
id: oracle-aggregator-rotation-silent-halt
title: A decode model with a hardcoded external contract list goes silently dry when the operator rotates its deployment — and every downstream run stays green
status: observed
scope: decode models whose contract_address is an inline list of externally-operated
  contracts (models/contracts/chainlink/contracts_chainlink_feeds_events.sql; by
  extension any oracle/aggregator/keeper decode where a third party controls the
  address set — Chainlink phase aggregators, celo_chainlink_feeds)
symptom: the decode table's max(block_timestamp) freezes on a specific minute while
  every daily run of the decode AND its downstreams completes green in seconds;
  downstream calendar-spine models keep emitting rows (forward-fill), so max(date)
  checks on THEM pass — only max() on the decode table or a last-real-observation
  check exposes the halt
last_verified: 2026-08-31
evidence:
  - "contracts_chainlink_feeds_events max(block_timestamp) froze at 2026-08-06 13:13:15; int_execution_prices_oracle_daily kept completing green in 2-5s every day after (Elementary run history 2026-08-25..30), discovered 2026-08-31 — 25 days later"
  - "execution.logs, topic0 0559884f… (AnswerUpdated): the 14 hardcoded aggregators' last events land 2026-08-05/06; a disjoint set of 44 emitters starts 2026-08-07 00:02 — Chainlink rotated every Gnosis feed to new AccessControlledOCR2Aggregator deployments (verified via Blockscout ABI + description() calls: GNO/USD 0xe2cd6230…, ETH/USD 0x89baf01d…, etc.)"
  - "masking layer: int_execution_token_prices_daily demotes native prices forward-filled > native_price_max_staleness_days (7) below off-chain feeds, so served USD prices stayed correct — which is exactly why nothing looked wrong for 25 days"
  - "second bite during repair: running the decode with the new addresses BEFORE their ABIs were in event_signatures appended ~1.9k rows with event_name='' and decoded_params={} and advanced the watermark to 2026-08-31; required gap_window_refresh.py --months 2026-08-01 to drop the partition and re-decode"
  - "fix in working tree 2026-08-31 (pending merge/deploy): new addresses appended in contracts_chainlink_feeds_events.sql + int_execution_prices_oracle_daily.sql mapping; ABIs via scripts/signatures/fetch_abi_to_csv.py x9 + targeted signature append; GNO daily closes continuous across the rotation boundary (105.54 -> 106.28)"
---

## Symptom
A decode model over a fixed list of third-party contract addresses stops appending —
its max(block_timestamp) freezes at a specific minute — while the model itself and
every downstream keep running green. Downstream models that forward-fill over a daily
calendar keep advancing their max(date), so the freshness sweep that catches most
halts passes. If a demotion/fallback layer (price hub staleness demotion) sits
downstream, even the served VALUES stay plausible, and the halt can run for weeks.

## Root cause
The address list names deployments the repo does not control. Chainlink feeds are
proxies over phase aggregators; the operator can (and on 2026-08-06/07 did, for every
Gnosis feed at once) deploy a new aggregator generation and repoint the proxies. The
old contracts simply stop emitting — no error, no tombstone event, nothing on our
side fails. `AnswerUpdated` kept the same topic0 across versions, so the halt is not
even visible as an unknown-topic pileup; the events continue under addresses the
model has never heard of.

## Forbidden action
Do not "fix" the staleness by re-running, full-refreshing, or gap-refreshing the
decode with the OLD address list — there is nothing to recover; the source moved.
And do not run the decode with NEW addresses before their ABIs are registered in
`event_signatures`: the decode LEFT-joins the ABI, so unmatched rows append with
empty event_name/decoded_params and the watermark advances past them — turning a
clean gap into a corrupt-rows repair (partition drop + re-decode).

## Detection
- Freshness must be checked on the DECODE table (max(block_timestamp)), never only on
  calendar-spine downstreams.
- Rotation signature: `SELECT address, min(block_timestamp), max(block_timestamp)
  FROM execution.logs WHERE topic0 = <event topic> AND block_timestamp > <halt - 2d>
  GROUP BY address` — the old set's max and a new set's min bracket the halt within
  hours.
- Map candidate new emitters to feeds with `description()` / Blockscout ABI before
  trusting them; exclude look-alikes (a `USDC / USD TEST CAPPED` contract emits the
  same topic).

## Safe remediation
Order matters:
1. Fetch + register ABIs first: `scripts/signatures/fetch_abi_to_csv.py <addr>` per
   new contract, regenerate/append `event_signatures` rows (see
   derived-seed-regen-unsafe before trusting a full regen), `dbt seed`.
2. Append the new addresses to the model's list and the downstream feed->symbol map.
3. Then run the decode. If step 3 ever ran before step 1: drop the affected month
   partition(s) via `gap_window_refresh.py --months <month> --select <decode>` and
   re-run.
4. Downstream oracle/native/hub models heal on their next normal run once the DEPLOYED
   image carries the new mapping — a local rebuild of an insert_overwrite downstream
   gets overwritten by the next cron running the old mapping.

## Ground truth
`execution.logs` is the authority: the events never stopped, they moved addresses.
Cross-check any new aggregator with an eth_call (`description()`, `latestAnswer()`)
against the served price before adding it.
