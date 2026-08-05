# models/celo/ — scoped guide

Gnosis Pay card activity on Celo (card provisioning, payments, funding,
balances, retention), built natively from decoded Celo execution logs.
Read with root AGENTS.md.

## Source of truth

- **Sources are native on-chain data**, not crawler- or Dune-fed: everything in
  this tree derives from `celo_execution` (blocks / transactions / logs),
  captured by the cryo-indexer deployment. The indexer runs in MINIMAL mode, so
  `traces`, `native_transfers`, `balance_diffs`, `storage_diffs` and `contracts`
  exist but stay **empty** — never build on them here.
- The backfill is **complete**: continuous coverage from the L2 migration block
  (31,056,500, 2025-03-26) through head, and the indexer now follows head. Source
  freshness is enforced (`celo_sources.yml`). Historical note: model comments used
  to tell you to `--full-refresh` because old months landed out of order. That is
  over; plain incremental is now the correct daily path.
- A flat or missing metric here is **not** a halted crawler (the pre-2026-08
  framing). Diagnose in this order:
  1. `celo_execution.indexing_progress` — completion and highest block per dataset.
  2. `int_celo_gpay_safe_registry` row count — did card discovery stall?
  3. The decoded layer (`int_celo_gpay_safe_events_native`, `_safe_transfers_alltoken`)
     against raw `celo_execution.logs` for the same window.
  See docs/lessons/raw-logs-ingestion-holes.md and
  docs/lessons/stale-snapshot-caveat.md.

## Card universe

- The registry is built from the **bridge fingerprint**: a Safe that emits
  `EnabledModule(<GP Roles proxy>)` has been provisioned as a GP card. It fires
  **pre-spend**, so the registry covers created-but-never-funded cards (the funnel
  top). The AggregateBridge is excluded from the card list — it is the settlement
  sink, not a card.
- **The settlement contract is a SEEDED SET — never hardcode a bridge address.**
  `seeds/celo_gpay_settlement_contracts.csv` is the single source of truth, read by
  `int_celo_gpay_roles_modules` (discovery), `int_celo_gpay_safe_registry`
  (self-exclusion) and `int_celo_gpay_activity` (classification). Two bridges are
  live and both are GP's: v1 `0xc4df5cac…` (from 2026-03-31, `status=migrating`) and
  v2 `0xc07cd8c2…` (from 2026-05-28, `status=active`). GP confirmed on 2026-08-05
  that v1 will be migrated onto v2. `0xd11e35ca…` is deployed, unused and
  unconfirmed — it sits at `status=planned` and is excluded from every filter.
  Adding GP's migration target must be a seed edit, nothing more.
- **Discovery and classification widen together, in the same run.** The registry
  decides which Safes exist; `int_celo_gpay_activity` decides what their transfers
  mean. Widening only the registry admits cards whose settlement transfers then fall
  through to the catch-all and book as **Withdrawals** — inflating withdrawals while
  still under-reporting payments, i.e. worse than omitting the cards.
- **Which bridge is a property of the TRANSFER, not of the card.** Use
  `int_celo_gpay_activity.settlement_address`. Because cards migrate, a per-card
  "generation" column is correct only until that card moves and silently wrong after.
  Migration state lives in `int_celo_gpay_roles_modules.wired_settlements`: a
  two-element array means that card has migrated. Zero had on 2026-08-05.
- Keying the tree on v2 alone until 2026-08-05 cost 235 cards, 1,743 transfers and
  ~$75.4k of volume, and — worse — made March–May read as empty, so a program running
  since March looked like it launched in June. Treat any Celo GP figure produced
  before 2026-08-05 as understated **and reshaped**; restate rather than splice.
- Corollary for any completeness check: start from the **seed**, never from a single
  bridge address. The earlier "every spender is in the registry" proof was circular —
  it derived the spender population from the one bridge the model already knew about,
  so a second bridge was undetectable. See
  `docs/lessons/circular-completeness-proof.md`.
- `int_celo_gpay_module_mastercopies` is an **independent deterministic
  cross-check, never an inclusion source**. GP provisions Roles proxies from more
  than one mastercopy, and at least one of those mastercopies is shared with
  unrelated projects, so it cannot be trusted to add cards — only to confirm that
  the fingerprint has not developed a gap. Of its 3491 rows, ~301 are proxies of
  unrelated projects that happen to share the `roles_pilot` mastercopy; the GP subset
  is the intersection with `int_celo_gpay_roles_modules`. Never aggregate this table
  as if every row were a GP card.
- **Mastercopy drift is a manual check, not an automated one.** The invariant is
  fingerprint ⊆ mastercopy: every GP Roles proxy should trace to a mastercopy this
  repo knows. It held at 100% on 2026-08-04. It is deliberately NOT a dbt test —
  a new mastercopy is legitimate GP activity, not a data defect, so it does not
  belong in a build-breaking gate. Run it by hand when Celo card counts look wrong
  or before trusting a cohort/growth figure:

  ```sql
  SELECT r.address, r.first_seen_at
  FROM int_celo_gpay_roles_modules r
  WHERE lower(r.address) NOT IN (
      SELECT lower(proxy_address) FROM int_celo_gpay_module_mastercopies
      WHERE module_type IN ('roles_patched', 'roles_pilot'))
    -- both sides full-rebuild: bound at the mastercopy table's watermark or
    -- freshly-provisioned cards read as false positives
    AND r.first_seen_at <= (SELECT max(created_at) FROM int_celo_gpay_module_mastercopies)
  ```

  Rows mean GP started issuing from an unknown mastercopy. Treat that as a prompt to
  re-derive the card universe — a new mastercopy generation has twice coincided with a
  new settlement contract — not merely to append an address. It returned **0 rows on
  2026-08-05**, once both bridges were in the seed; the 532 rows seen beforehand were
  the v1 cohort plus unrelated projects. Never invert the direction: mastercopy ⊆
  fingerprint is FALSE, as `roles_pilot` is shared with unrelated projects.
- Funnel stages are three different populations and are routinely confused:
  issued (registry) > funded (received any inbound token) > activated (made a
  payment). Roughly 1490 / 815 / 476 on 2026-08-03, with issuance running ~10
  cards/hour, so treat any absolute count in this repo's prose as a snapshot and
  re-derive it. `cumulative_funded` in `fct_celo_gpay_activity_daily` counts from
  first **payment**, so it tracks activated, not funded — do not read it as funded.

## Invariants

- **Keep Celo and Gnosis gpay metrics separate.** Same product, different chain:
  never sum or blend them in a mart without an explicit chain dimension;
  cross-chain totals are a presentation-layer decision.
- **No cumulative (`{{ this }}`) models in this tree** — reprocessing must be
  order-free; monthly insert_overwrite is safe per partition
  (docs/lessons/backfill-order-cumulative.md).
- **Value flows at transaction time, stocks as of the reporting date.** A flow's
  `amount_usd` is priced when it happened (`int_celo_gpay_activity`) and is correct.
  A balance must be the native running total valued at that date's price
  (`fct_celo_gpay_balances_safe_daily`, mark-to-market via ASOF join). Never build a
  balance by accumulating flow-USD: that is a cost basis, it drifts without bound for
  a volatile token, and it can go negative while the card still holds the asset. Only
  USDT/USDC have ever moved, so the two differ by ~0.02% today — the whitelisted RWA
  token XAUt0 is what makes the distinction load-bearing.
- **Never scope a headline balance with a hardcoded symbol list.** Partition on
  `token_class` from `celo_tokens_whitelist` (`fct_celo_gpay_snapshots`: STABLECOIN =
  TotalBalance = spendable float, RWA = RewardBalance). A symbol list silently drops
  the first newly-transacting token instead of failing. Distinguish "no position"
  (0) from "position held but unpriced" (NULL) — the price hub does not forward-fill,
  so a $0 tile would be a plausible-looking lie; the ASOF carry-forward is uncapped
  and the two warn tests on `fct_celo_gpay_balances_safe_daily` are what make it safe.
- **Celo tests belong in the model's own `schema.yml`, never in `tests/`.** That
  folder is reserved for platform-wide invariants; a per-project assertion goes next to
  its model as a `dbt_utils.expression_is_true` (use `config.where` to scope it, as the
  two valuation tests on `fct_celo_gpay_balances_safe_daily` do). The mastercopy-drift
  check is deliberately not a test at all — see the manual probe above.
- **`join_use_nulls` is 0 by default.** An unmatched LEFT JOIN row yields `''`
  for a String and `0` for a number, so `right.col IS NULL` never fires and
  `IS NOT NULL` is always true. This has already produced one inert test and one
  unreachable churn segment in this tree. Use `NOT IN` / `= ''`, or set
  `join_use_nulls = 1` in a pre-hook and keep it consistent across the model.
  Read docs/lessons/ch-left-join-nulls.md before writing any outer join here.
- **The heavy models scan ALL Celo Transfer logs.** Never
  `dbt run --full-refresh` `int_celo_gpay_safe_transfers_alltoken`,
  `int_celo_gpay_safe_events_native` or `contracts_celo_chainlink_feeds_events`
  directly — the single-query scan OOMs (ClickHouse code 241). Rebuild through
  `scripts/full_refresh/refresh.py` in the monthly batches declared in
  `meta.full_refresh`. The scoped-append path only fills EMPTY months; to
  reprocess a populated month, drop its partition first
  (docs/lessons/staged-insert-overwrite-wipe.md,
  docs/lessons/append-over-populated-duplicates.md).
- Wallet-recognition timing: a Safe can be recognized shortly AFTER its first
  activity lands; the activity models rely on ReplacingMergeTree latest-row
  semantics to reconcile — don't "fix" apparent same-key duplicates by hand,
  read docs/lessons/ch-merge-semantics-primer.md first.
- **Whitelist scope.** `celo_tokens_whitelist` carries four tokens (USDT, USDC,
  USDm, XAUt0), and priced/whitelisted models cover all four. Only USDT and USDC
  have ever moved on a card as of 2026-08-03 — USDm and XAUt0 paths are real code
  on zero rows, so treat any claim about them as untested. Note USDm is the
  rebranded cUSD at `0x765de816845861e75a25fca122bb6898b8b1282a`, which most
  external tooling still labels cUSD. The all-token models
  (`_safe_transfers_alltoken`, `fct_celo_gpay_card_funding`,
  `fct_celo_gpay_card_balances_alltoken_daily`) deliberately include
  non-whitelisted tokens with NULL `amount`/`amount_usd`; do not sum those columns
  without filtering.
- **The all-token daily balance model is sparse** (rows only on days with flow),
  unlike the densified `fct_celo_gpay_balances_safe_daily`. "Latest balance"
  requires `argMax(balance, date)` per Safe, never `WHERE date = max(date)`
  (docs/lessons/sparse-zero-row-stale-survival.md).

## Prices

- `int_celo_token_prices_daily` is the single price hub. Priority: Chainlink
  `AnswerUpdated` decoded from native logs, then a Dune off-chain fallback for
  what native cannot reach, then a $1 peg for the card stablecoins. XAUt0 has no
  direct Celo feed and is derived as `CELO/USD ÷ (CELO/XAUt)` from Mento
  SortedOracles (CGP-0240, live 2026-06-09).
- The Chainlink USDT and USDC aggregators only start **2026-06-23**, so earlier
  stablecoin history legitimately resolves via Dune or the peg. Check
  `price_source` before concluding a feed is broken.
- Forward-fill is not applied. Every (date, symbol) is the last answer actually
  observed that day, so a symbol can be absent on a day with no oracle update.

## Validation

- `python scripts/checks/run_all.py`; `dbt test -s tag:celo` (or the gpay
  subtree selector) after any change.
- After adding or renaming a model, regenerate the semantic entity overlay:
  `dbt docs generate` (warehouse-connected, for `target/catalog.json`) then
  `python scripts/semantic/generate_entities.py --target-dir target`. New models
  carrying `safe_address` / `token_address` / `token_symbol` are not joinable
  through the semantic hubs until that generated overlay is refreshed, and
  `run_all.py --full`'s `entity-overlay` step fails while it is stale.
