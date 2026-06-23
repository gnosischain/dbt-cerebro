# Model review: execution/lending

> **Status (updated 2026-06-23):** review COMPLETE. Triaged against prod `dbt`, verified, and
> acted on. All correctness defects are fixed or proven-latent-with-zero-live-reach; the
> treasury/contract pollution is resolved by the most-correct design (see below). Only one
> operational apply step and optional future enhancements remain.
>
> **Is lending ready? Yes** — the code/models are correct and the semantic layer is in place.
> The single remaining gate is operational: the data team must apply the
> `contracts_whitelist` seed + rebuild `non_user_contracts` in prod for the revenue-side
> cleanup to land (lending marts are already correct and unaffected).

---

## Scope

DeFi lending on Gnosis Chain for **Aave V3** and **SparkLend** only. **Agave is intentionally
excluded** (Aave V2 fork, effectively wound down — 0 deposits in 90 days, only housekeeping logs,
and its decoded events are blank/unusable). 5 intermediates, 4 facts, ~10 api marts. Exact Int256
WadRayMath accounting; ASOF-join RDU pairing that correctly handles SparkLend's FlashLoan RDU storm.

---

## Resolution log

### Done (implemented this pass)

- **Utilization Int256 underflow fixed.** `int_execution_lending_aave_utilization_daily` now clamps
  `greatest(cumulative_scaled_borrow, 0)` before the unsigned cast, so a rounding-negative borrow
  reads ~0% instead of wrapping to ~e30. Verified after refresh: 0 absurd rows (was 1), max
  utilization 108.61.
- **balance_usd=0 price-gap detector added.** `expression_is_true(balance_usd > 0 OR balance <= 0)`
  on `int_execution_lending_aave_user_balances_daily` (`severity: warn`, recent window) so a
  price-feed lag surfaces loudly instead of silently zeroing TVL.
- **Borrowers KPI description corrected.** `api_execution_lending_borrowers_count_7d` now documents
  it as a 7-day FLOW (not a debt-balance STOCK) and flags that it is NOT symmetric with the lenders
  count (a point-in-time balance STOCK). Numbers were already correct; only the doc was wrong.
- **Agave scope documented** on `int_execution_lending_aave_daily` (Aave V3 + SparkLend only).
- **Protocol contracts excluded from revenue** (added to `seeds/contracts_whitelist.csv` ->
  `non_user_contracts`): the Aave V3 collector/treasury `0x3e652e…`, CoW Settlement `0x9008d19f…`,
  and the ATokenVault `0x9f40ca84…`. These were being counted as "users" (the treasury verified
  leaking 958 rows into `int_revenue_sdai_fees_daily`). Because the lending marts do NOT anti-join
  `non_user`, this cleans the **revenue** active-user counts while correctly leaving **lending TVL**
  intact (those aTokens are real supplied liquidity). Takes effect after
  `dbt seed --select contracts_whitelist` + rebuild of `non_user_contracts`.
- **Lending semantic layer expanded.** Added semantic models + metrics for TVL by token, active
  lenders count, active borrowers count, and balance cohorts (value + holders). Count metrics carry
  the `protocol='ALL'`-row caveat (don't sum across protocol) and the STOCK-vs-FLOW non-comparability
  note. All `quality_tier: candidate`.

### Verified — reduced or no change needed

- ~~CRITICAL "Int256 underflow live on 14 WxDAI rows / -975T drift"~~ — the large WxDAI drift had
  already cleared via a refresh; only 1 stray historical row remained (a `-2` repay-rounding
  residue), now handled by the clamp. Downgraded from CRITICAL.
- ~~HIGH "balance_usd=0 understating TVL 51-100%"~~ — was a transient price-feed lag on 2026-06-07;
  latest date is clean. Detector added so a recurrence is visible.
- **No lending `non_user` anti-join.** Considered and rejected: excluding the treasury/vault would
  understate TVL by ~$9.8M (5.8% of Aave V3's $169.9M latest-date TVL) — those aTokens are real
  supplied liquidity. The only metric where exclusion is conceptually right (lender *count*) is
  affected by just 3 of 31,057 lenders (0.01%). TVL legitimately includes all suppliers; revenue
  (which anti-joins `non_user`) correctly excludes the contracts. **One contract behavior gives
  both metrics their correct answer simultaneously** — see "Most-correct design" below.
- **ATokenVault look-through not built.** `0x9f40ca84` is an ERC-4626 wrapper whose `asset()` is
  the **deprecated old-EURe** (Monerium v1, `0xcB444e90…`, replaced 2024-08-25) held in Aave. It
  has only ~3 distinct holders (high internal churn — ~8.5k transfers / 3 receivers in 30 days). A
  full ocsDAI-style look-through (decode events + share price + whitelist + backfill) for 3 holders
  of a deprecated asset is wildly disproportionate — it would add at most ~3 users to the headline.
  Excluding it as a non-user is the correct, proportionate call; logged as a low-priority candidate
  to revisit only if its holder base grows.

### Most-correct design (final decision, 2026-06-23)

The treasury/contract question was driven to its most-correct resolution, and it is **already
the implemented state — no new pipeline is needed**:

1. **Three protocol contracts excluded from revenue** (via `contracts_whitelist` → `non_user`):
   the Aave V3 collector/treasury, CoW Settlement, and the ATokenVault. None are end-users, so
   they correctly drop out of revenue active-user counts.
2. **Lending TVL/counts deliberately do NOT anti-join `non_user`.** This is correct, not an
   oversight: lending TVL must reflect all supplied liquidity. The same seed addition therefore
   cleans revenue while leaving lending TVL intact.
3. The only residual is the ~$7.7M of (deprecated) EURe behind the ATokenVault not being
   re-attributed to its ~3 holders in revenue — immaterial to the headline, logged not built.

Net: the split is internally consistent and maximally correct with a single change. The earlier
review's "add a treasury exclusion to the lending diffs/top-lenders/TVL" recommendation is
**explicitly superseded** by this analysis (it would have corrupted TVL to fix a 0.01% count).

---

## Open items

### Operational — must apply (the one readiness gate)

- **Apply the `contracts_whitelist` seed + rebuild `non_user_contracts` in prod.** The three
  contract exclusions are committed in the seed but only take effect after
  `dbt seed --select contracts_whitelist` + a rebuild of `int_execution_accounts_non_user_contracts`
  (then the revenue models pick it up on their next run). Lending marts are already correct and do
  not depend on this. (Data-team run.)
  - **Cross-stream impact verified (2026-06-23, playground_max).** `non_user_contracts` feeds all
    five revenue fee streams, so the impact was checked across all of them. The three contracts
    appear in **only** `holdings` and `sdai` (the on-chain balance streams) and in **NONE** of the
    human-facing product streams (`gpay` = 0 rows, `gnosis_app` = 0 rows). So the exclusion strips
    only protocol/infra balances, never legitimate product users. Footprint removed (cumulative,
    full history): ATokenVault holdings 218 user-days/$390 fees; treasury holdings 1,586/$232 + sDAI
    909/$10; CoW Settlement holdings 2,363/$1.48 + sDAI 937/$1.40 — total ~$635 of fees over ~2.5
    years. The treasury's `~958 rows / ~$13` sDAI leak verified earlier reconciles (909/$10 here is
    the stale playground window). The ATokenVault's EURe balance leaves the holdings base when
    excluded (the documented, immaterial under-count) and never touched sDAI (it wraps EURe, not sDAI).

### Documentation — caveats that must travel with figures

- **Reporting caveats**: lending supply is sum-of-transfers, not on-chain `totalSupply()` (diverges
  for vault/rebasing tokens); OTHERS supply excludes Aave/Spark wrapper balances by design
  (`symbol_exclude`); sDAI is both a SparkLend reserve and a Savings-xDAI asset — the scope boundary
  is documented (no in-repo aggregator double-counts it), keep it documented for any external rollup.

### Optional future enhancements (not blocking)

- **Top-lenders stack is `dev`-tagged** (excluded from CI/prod). If ever promoted, label/exclude the
  treasury + ATokenVault so they don't dominate the ranking, and the `non_user` question reopens
  there. Decide promote vs keep-internal explicitly.
- **ATokenVault look-through** — low-priority candidate; only worth building if its holder base grows
  meaningfully (today ~3 holders of a deprecated asset).
- **Semantic coverage** — utilization and top-lenders are still not in the semantic layer
  (utilization would need a dedicated api view first, or be keyed by raw `token_address`).
- **Cosmetic/cleanup** (no behavior change): dead `is_incremental()`/`lka`-JOIN branches in
  `int_execution_lending_aave_daily`; duplicate `'lending,lending'` tags; `max(date)-7` vs
  `subtractDays`; prune the duplicate auto-generated weekly APY candidate metrics; add grain/range
  tests to the Int256 intermediates.
- **Agave** — intentionally excluded and dormant (~88 events/90d, ABI never loaded). Only revisit if
  the protocol reactivates; document the exclusion meanwhile (done on `int_execution_lending_aave_daily`).
- **Latent: doubled `2026-06-18` partition** in `int_execution_lending_aave_user_balances_daily`
  (recovery refill append side-effect — 2x rows for that one date, 310 grains with conflicting
  `balance_usd`). Unreachable by live KPIs (all marts read `max(date)`, which has since advanced), and
  RMT won't self-heal it correctly (no version column → nondeterministic merge). Needs a one-off
  force-merge/DELETE only if that historical date is ever queried directly; pair with a grain-uniqueness test.

---

## Pros (retained for reference)

- On-chain-faithful Int256 WadRayMath (`rayDivFloor`/`rayDivCeil`), ASOF-join RDU pairing that
  correctly handles SparkLend's FlashLoan RDU storm, bitmap dedup for active-user counts.
- Clear canonical definitions (supply/borrow APY, utilization, cohorts) and documented forward-fill.
- Healthy grain integrity on the core daily model (zero duplicates).
- Contract addresses, aTokens, and reserve mappings verified against seeds and the docs site.
