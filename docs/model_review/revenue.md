# Model review: revenue

> **Status (updated 2026-06-22):** triaged against prod `dbt` and acted on. Stale/refuted
> findings removed; implemented items recorded below; only genuinely open items remain.

---

## Scope and inventory

| Layer | Count | Notes |
|---|---|---|
| `models/revenue/intermediate/` | 8 SQL files | 4 daily per-stream + 1 ocsDAI look-through, 1 unified daily view, 1 weekly per-user, 1 monthly per-user |
| `models/revenue/marts/` | ~42 SQL files | api_ thin wrappers + fct_ cohort/totals/per-user models |
| `semantic/authoring/revenue/` | semantic models + metrics | gnosis_app now covered (added 2026-06) |

The revenue unit models **potential** economic value to the Gnosis DAO from on-chain fee
streams: imputed interest on stablecoin holdings (EURe, USDC.e, BRLA, ZCHF), imputed sDAI
yield (sDAI + Aave aGnosDAI + OpenCover OC-sDAI look-through, 10% DAO share), Gnosis Pay
per-payment fees, and Gnosis App (Metri CRC) fees. Cross-stream dedup is by `user_pseudonym`.
Headline output: cross-stream "economically active" users (weekly ≥ $6/52w, monthly ≥ $0.50/mo).

---

## Resolution log

### Done (implemented this pass)

- **ocsDAI (OC-sDAI) added to the sDAI stream.** New `contracts_ocsdai_events` (decoded
  ERC-4626 Deposit/Withdraw), `int_yields_ocsdai_share_price_daily` (share price = assets/shares,
  forward-filled), `int_revenue_ocsdai_user_balances_daily` (shares × share_price × sDAI price),
  UNIONed as a third branch into `int_revenue_sdai_fees_daily`; OC-sDAI added to
  `tokens_whitelist` (which also auto-excludes the vault as a non-user, fixing the prior
  single-"user" leak). Verified: look-through total tracks the vault's on-chain sDAI within
  ~1-3%; ~580 holders attributed individually instead of collapsing into the vault address.
- **sDAI rate join INNER → LEFT + COALESCE.** A missing rate date (the 7-day launch warmup,
  confirmed dropping 7 days in prod; or any freshness lag) no longer silently drops sDAI
  user-days; the balance row is preserved with `fees=0` until a rate exists. Added a `not_null`
  test on `fees`. This also closes the NULL-fee propagation concern (COALESCE keeps fees non-NULL).
- **gnosis_app added to the semantic layer.** `has_gnosis_app` dimension added to
  `revenue_per_user_weekly`/`_monthly`; `revenue_gnosis_app_cohorts_weekly`/`_monthly` semantic
  models + metrics added. (Prod has `has_gnosis_app=1` on 223k weekly / 33k monthly user-rows.)

### Verified — no change needed

- **10% sDAI DAO share** confirmed correct (applies to sDAI, Aave aGnosDAI, and OC-sDAI).

### Dropped (stale or refuted vs prod data)

- ~~CRITICAL "monthly pipeline ~75% of history missing"~~ — already fixed before the review
  (`partition_by='month'`, not `toStartOfYear`); prod has 32 continuous months. No issue.
- ~~HIGH "GPay settlement address broken post-April 2025"~~ — refuted; fees flow continuously
  and grow through 2026. The review conflated the Spender router with the fee-settlement target.
- ~~"NULL fees propagate"~~ — gnosis_app NULL was a transient dev partial-month artifact (0 in
  prod); gpay GBPe was 8 historical launch-week rows. Addressed defensively by the sDAI COALESCE.

---

## Open items

### Medium

- **Document the cohort-vs-totals threshold split.** Totals floor at $0.50 (weekly $6); cohorts
  floor at $0.01 and the cross-stream weekly cohort uses `include_below_one=true` while per-stream
  cohorts use `>= 1`. Numbers are individually correct, but cohort `users_cnt` is **not summable**
  to the headline totals (~1.6x overstatement). Add a "reconciling cohorts vs totals" note to the
  `schema.yml` descriptions of the four cohort marts and mirror it into the cohort semantic models.
- **`refill_safe_hooks` on holdings & sDAI daily models.** Both carry the `refill_append` tag and
  manual `SET` hooks but are **missing `max_memory_usage = 8 GiB`** (they have the spill settings +
  grace_hash but not the ceiling), so a whole-month refill can OOM (Code 241) instead of spilling.
  Replace the manual hooks with the `refill_safe_hooks` macro. (gpay/gnosis_app are tiny — skip.)
- **Hardcoded APY rates** (EURe/USDC.e/BRLA/ZCHF) remain compile-time Jinja constants with no
  effective-date/audit trail. Consider moving to a dbt var or config seed. (The 10% sDAI share is
  verified, but the same mechanism would benefit the holdings rates.)

### Low / cleanup (opportunistic)

- Add `dbt_utils.unique_combination_of_columns` tests on `int_revenue_fees_weekly_per_user` and
  `int_revenue_fees_unified_daily` (the only intermediates without grain tests).
- Migrate `int_revenue_fees_weekly_per_user` off `delete+insert` (project-banned, currently on
  `no_delete_insert.allow` as acknowledged debt) to `insert_overwrite`.
- BRLA: ~31% of holdings rows have `balance_usd_total = 0` after rounding; filter dust
  (`balance_usd_total > 0`) so zero-economic rows don't inflate BRLA user-day counts.
- Remove the dead `countIf(month_fees > 0)` under `WHERE month_fees >= 0.01` in the monthly cohort
  models (misleading, no wrong numbers).
- API convention backlog: all `api_revenue_*` views are allowlisted in `check_api_tags.allow`
  (56 entries) with no typed columns blocks; weekly fct views read ReplacingMergeTree without
  `FINAL` (latent dup risk, none observed). Highest effort, lowest value — defer unless doing a
  dedicated API-convention pass.

---

## Pros (retained for reference)

- Cross-stream dedup is architecturally correct — fees summed per user before thresholding, so
  double-counting is impossible in headline active-user metrics.
- Privacy boundary well-enforced at the mart layer (`pseudonymize_address`, tier3 +
  `allow_unfiltered:false` on per-user endpoints).
- Weekly densification (`arrayJoin` calendar + `ROWS BETWEEN 51 PRECEDING`) correctly anchors the
  52-week rolling window regardless of activity gaps.
- "Potential not realised" framing documented in `schema.yml` and propagated to semantic models.
- Stream architecture is extensible — OC-sDAI was added as a look-through into two intermediates
  without touching any downstream mart or semantic model.
