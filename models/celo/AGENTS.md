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
  live and both are GP's: `settlement_legacy` `0xc4df5cac…` (from 2026-03-31,
  `status=migrating`) and `settlement_current` `0xc07cd8c2…` (from 2026-05-28,
  `status=active`). GP confirmed on 2026-08-05 that the legacy one will be migrated
  onto the current one. They share ZERO event signatures, so decoding them needs TWO
  separate ABIs and any settlement model must handle both shapes. **Why** they share
  none was settled on 2026-08-06 when the ABIs were fetched, and it is not that they
  are unrelated systems: both are verified as `AggregateBridge` with identical
  function selectors (`settle`, `USDC`, `USDT`, `USDT_OFT`, `quoteNativeBridgeFee`),
  and every event on the current one adds an indexed `sender` as its first parameter,
  which changes the canonical signature and therefore the topic0. Two generations of
  one design. The `legacy`/`current` labels stay — they describe lifecycle without
  claiming a release lineage GP has never published — but the earlier note here that
  these were "two different contracts, not two versions of one" was wrong on the
  substance, and the event counts it quoted (5 and 7) were events that had *fired*,
  not events *declared* (6 and 8).
  `0xd11e35ca…` is deployed, unused and
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
- Keying the tree on the current contract alone until 2026-08-05 cost 235 cards, 1,743 transfers and
  ~$75.4k of volume, and — worse — made March–May read as empty, so a program running
  since March looked like it launched in June. Treat any Celo GP figure produced
  before 2026-08-05 as understated **and reshaped**; restate rather than splice.
- Corollary for any completeness check: start from the **seed**, never from a single
  bridge address. The earlier "every spender is in the registry" proof was circular —
  it derived the spender population from the one bridge the model already knew about,
  so a second bridge was undetectable. See
  `docs/lessons/circular-completeness-proof.md`.
- **GP's own charge record is now decoded, and the inference matches it exactly.**
  `contracts_celo_gpay_settlement_events` decodes both settlement contracts (ABIs
  fetched 2026-08-06). Its `TokenPullSuccess` is what GP itself records for each card
  charge, so it is a completeness proof from the *operator's* side of the boundary
  rather than from our own anchor. Counted inside `int_celo_gpay_activity`'s
  `max(block_time)` it equals that model's `Payment` rows exactly — **5,165 v 5,165**,
  zero per-day variance, holding independently per contract (1,743 legacy, 3,422
  current). Always bound the comparison at that watermark: settlement batches land at
  01:00 and 13:00 UTC and the activity model is built at a point in time, so an
  unbounded count always shows a spurious surplus (172 on 2026-08-05, every one of
  them simply later than the build). **Re-run this after any change to the Payment
  CASE** — it is the only external check the classifier has.
- The decoded layer also carries three things nothing else can see: failed charge
  attempts (`TokenPullFailedWithAmount`, 7 so far — they move no money, so a
  transfer-derived model cannot see them by construction), the treasury leg
  (`SettlementBridged` cross-chain via the LayerZero USDT0 OFT vs `SettlementTransferred`
  settled locally, including the receiver change away from `0x8ab54f9e…` after
  2026-06-10), and the CELO cost of settling (`NativeBridgeFeePaid`, 1,458 CELO across
  451 batches). Value is conserved end to end: charges in minus settlements out is
  0.00 on both contracts.
- **Adding a settlement contract to the seed REQUIRES fetching its ABI in the same
  change** — `python scripts/signatures/fetch_abi_to_csv.py 0xNEW --chain celo --regen`.
  Discovery, classification and decoding all read that seed, so a seed-only edit leaves
  the decode layer emitting rows with `event_name = NULL`. That is deliberate (a
  visible null beats an absent contract) but it is not a state to ship.
- **Never trust a regenerated signature seed on the generator's exit code.** Diff it as
  a SET against HEAD and require zero removed rows. Running the generator on
  web3 v7 used to silently truncate every topic0, and the committed seed used to hold
  24 in-use rows that no ABI could reproduce — both fixed 2026-08-06, both silent while
  armed. `docs/lessons/derived-seed-regen-unsafe.md`. The generator now writes in a
  deterministic identity order (chain, then address, name, signature), so a
  regeneration that adds one ABI produces a diff of that size rather than reshuffling
  6,000 lines; the set-diff check stays mandatory regardless, since ordering says
  nothing about content.
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
  the legacy cohort plus unrelated projects. Never invert the direction: mastercopy ⊆
  fingerprint is FALSE, as `roles_pilot` is shared with unrelated projects.
- Funnel stages are three different populations and are routinely confused:
  issued (registry) > funded (received any inbound token) > activated (made a
  payment). 1817 / 1087 / 662 on 2026-08-05, with issuance running ~10 cards/hour, so
  treat any absolute count in this repo's prose as a snapshot and re-derive it.
  **The confusion was in the models, not just the prose.** Until 2026-08-05 the
  `fct_celo_gpay_activity_{daily,weekly,monthly}` column `cumulative_funded` counted
  from first **payment**, so `api_celo_gpay_funded_addresses_*` charted activation
  under the funded label (654 vs a true 1075) and `api_celo_gpay_total_funded` served
  the `PaymentUsers` snapshot (662 vs 1087). Both now mean funded; the original series
  survive as `cumulative_activated` /
  `api_celo_gpay_activated_addresses_*` / `api_celo_gpay_total_activated`. Two rules
  follow:
  - **Never derive "funded" from an outbound action.** Funded is inbound
    (`Top-up`/`Reversal`/`Cashback`); activated is outbound (`Payment`). If a metric
    name says funded, its filter must be an inbound action.
  - **A daily fact's date spine cannot be one action type.** The activity models'
    spine is the UNION of payment days and funnel-transition days, because 9 days had
    a card funded and nobody spending, and a payment-only spine silently dropped the
    28 cards funded on them — the LEFT JOIN had no row to attach them to. Rows from
    the union carry `active_users = total_payments = total_volume_usd = 0`, which is
    the truth for those days, not missing data.
  - The **Gnosis twin still has the original Payment-derived `funded`** and so still
    understates funding. Fixing it needs the same split plus a dashboard change; it is
    knowingly deferred, so do not treat Celo/Gnosis funded as comparable.
- **Never quote a conversion rate without age-normalising it.** funded / issued over
  the whole card base is not a conversion rate while issuance is ramping: most cards
  are young, so "unfunded" mostly means "issued recently", and the ratio moves when
  issuance accelerates even if behaviour is identical. On 2026-08-06 the naive figure
  was 59.8% against a pooled 7-day rate of 54.9% and a 30-day rate of 78.5% — the naive
  number sits between two very different truths and equals neither.
  `fct_celo_gpay_card_funnel` (one row per card) and
  `fct_celo_gpay_funnel_cohorts_monthly` (issuance cohorts, 7/30-day windows) exist so
  this is done consistently. Three rules:
  - **Gate every rate on `observation_days`.** A card enters the 7-day rate only once
    it has 7 days of observation. `eligible_7d`/`eligible_30d` are the denominators,
    not `cards_issued`, and `cohort_complete_*` says whether a row is final.
  - **Clip all three funnel signals to one horizon.** `int_celo_gpay_wallets` rebuilds
    fully every run and reaches head while `int_celo_gpay_activity_daily` is
    incremental and lags, so a card can be spend-visible before it is funding-visible.
    `observed_through` (max date in the activity model) is the binding constraint;
    without it the newest cohort always looks like it converts worst.
  - **The `_ever` columns are not comparable across cohorts** and the lag medians are
    completer-biased — a young cohort's median only sees its fast converters, so
    medians drift up as cohorts mature.
- **Settlement is atomic — there is no lag and no float, so do not model them.** All 476
  settlement transactions contain both the `TokenPullSuccess` charges and the outflow;
  not one does only one of the two. Cards are charged and the money leaves in the same
  transaction, so charge-to-settlement time is zero by construction. Lag and float marts
  were scoped and abandoned on 2026-08-06 for this reason. What IS worth measuring is
  cost: `fct_celo_gpay_settlement_batches` and `api_celo_gpay_settlement_cost_monthly`
  carry the LayerZero fee per batch, which has fallen from $0.205 to $0.0074 per charge
  (107.6 bps to 1.19) as batch size grew from 1.3 to 18.2 charges. Batches run ~7x daily,
  not twice as previously recorded.
- **Never take a per-token split from `TokenPullSuccess`.** `settlement_legacy` reports
  the wrong token on every one of its 1,752 pulls — all decode as USDC, while the chain
  shows 345 USDC and 1,407 USDT. Amounts and counts are right; only the label lies, and
  it lies consistently, so nothing looks broken. `settlement_current` is fine. Take the
  amount from the event and the token from the ERC-20 `Transfer`, and check value
  conservation at contract level (where it holds at 0.00) rather than per token.
  `docs/lessons/event-field-can-lie.md`.
- **Roles allowances are not a constraint and are not modelled.** Decoding `SetAllowance`
  and `ConsumeAllowance` on the Roles modules shows every card on the same uniform
  $20k daily cap, and no card has come near it. "Remaining allowance" would therefore
  be a constant dressed up as a metric. Revisit only if the cap is ever tiered or a
  card actually hits it.
- **CIP-64 is not a MiniPay label.** `transaction_type = 123` is Celo's fee-currency
  envelope; most of the chain uses it. Card funding is classified as a shape in
  `int_celo_gpay_funding_tx_envelopes.funding_channel` /
  `fct_celo_gpay_card_funding.funding_channel` /
  `fct_celo_gpay_card_funnel.first_fund_channel`: `cip64_direct_solo` (CIP-64 + EOA
  `transfer()` + funder funds exactly one card — MiniPay-shaped), `other_direct`
  (same call shape, non-CIP-64 envelope), `hub` (funder funds 2+ cards; fan-out wins
  even when every transfer is CIP-64), `mediated` (Safe/router path), `mixed` /
  `unknown`. Do not expose `is_minipay = (type = 123)`. Glossary lives on those
  models' schema.yml descriptions.

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
