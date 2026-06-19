# GA -> GP top-up capture probe

Date: 2026-06-17. Follow-up to the growth-loop analysis that flagged the `topup` cohorts as
suspiciously tiny. **Resolved 2026-06-17** — the model was redefined off the wallet bridge (see
Resolution at the end); the probe figures below are the pre-change baseline that motivated it.

## Question

Are GA-acquired users topping up their Gnosis Pay card? The growth analysis saw only ~59-203
top-up users and suspected under-capture.

## What the current model captures

`int_execution_gnosis_app_gpay_topups` defines a top-up as a **same-transaction** CoW-trade +
GP `Crypto Deposit`, where the bought token equals the deposited token, on whitelisted tokens,
for identified GA users, from 2025-11-12.

| Captured (current model) | Value |
|---|---|
| Top-up events | 131 |
| Distinct GA users | 59 |
| Distinct GP wallets | 73 |

## The real GA -> GP funding population

Using the **persistent** GA<->GP ownership bridge `int_execution_gnosis_app_gpay_wallets`
(`is_currently_ga_owned`, `onboarding_class`) instead of the transactional same-tx link:

| GP `Crypto Deposit` population (from 2025-11-12) | Wallets | Events |
|---|---|---|
| All deposits (entire Gnosis Pay) | 32,701 | 348,140 |
| Into **GA-owned** wallets (`is_currently_ga_owned`) | **1,073** | 49,409 |
| Into **onboarded-via-GA** wallets | 285 | — |
| of which **captured** by the current top-up model | **15** | 68 |
| of which **missed** | **1,073** | 49,341 |

The current model captures roughly **1.4% of GA-owned GP wallets that actually receive funding**
(15 of 1,073), and ~0.1% of funding events. The 73 "captured" wallets and the 1,073 currently-GA-owned
funded wallets barely overlap (only 15) — the same-tx model and the ownership bridge largely disagree
on which wallets are GA-attributable.

## Why it misses — NOT a whitelist problem

Token mix of the missed deposits into GA-owned wallets:

| Token | Missed events | Missed wallets | Missed USD |
|---|---|---|---|
| EURe | 46,728 | 996 | ~6.64M |
| GBPe | 842 | 44 | ~0.32M |
| GNO | 693 | 261 | ~0.15M |
| USDC.e | 1,011 | 83 | ~0.11M |
| WETH / others | ~65 | — | small |

The dominant missed token is **EURe, which is already whitelisted** (~$6.6M across 996 wallets).
So the gap is **not** the token whitelist — it is the **same-transaction CoW-trade requirement**.
Most GA users fund their card by sending EURe (or another token) **directly** into the GP wallet,
or via a non-CoW route, rather than by an atomic swap-and-deposit in a single transaction. The
current model only catches the narrow atomic-swap flow.

## Recommendation (needs a product decision before implementing)

Redefine `topup` around the **persistent GA<->GP wallet bridge**, not the transactional same-tx
join. Attribute any GP `Crypto Deposit` into a GA-owned wallet as a top-up. Open product calls:

1. **Population.** Broadest = any wallet `is_currently_ga_owned` (1,073 funded wallets). Stricter,
   cleaner "acquired user" = `onboarding_class = 'onboarded_via_ga'` (285 funded wallets). Pick which
   defines an "acquired user top-up".
2. **Self-funding vs external.** `Crypto Deposit` is inbound funding; decide whether to exclude
   deposits whose `counterparty` is the user's own GA address (moving own funds) vs genuine external
   top-ups, if that distinction matters for the metric.
3. **Keep the swap linkage?** The same-tx CoW linkage is still useful as an *enrichment* (which
   top-ups came from an in-app swap) but should not be the *gate* for what counts as a top-up.

Downstream that would change once redefined: `int_execution_gnosis_app_user_activity_daily`
(`topup_rows`), `fct_execution_gnosis_app_gpay_topups_cohort_monthly`, the time-to-first-conversion
view's `topup` series, and the retention-by-action `topup` curve.

## Resolution (implemented 2026-06-17)

`int_execution_gnosis_app_gpay_topups` was redefined to: any GP `Crypto Deposit` into a
**currently-GA-owned** wallet (`is_currently_ga_owned`), `ga_user` = the wallet's first GA owner,
from 2025-11-12 on. Population chosen: **broadest (all GA-owned wallets)**. The same-tx CoW-trade
gate was removed. Downstream column contract preserved (`token_bought_*` kept as legacy names for
the deposited token; synthetic per-tx `log_index` keeps the dedup key unique — `unique_combination`
test passes). Full-refreshed and the 74 downstream models rebuilt via `scripts/full_refresh/refresh.py`.

Result: 73 → **1,073 wallets**, 59 → **1,072 GA users**, 131 → **49,409 events**, **$7.2M**.
Topup converters in `first_conversion` 59 → 1,072; time-to-first-conversion topup `pct_converted`
~0.2% → 2.2–8.1%; topup cohort sizes 1–27 → 81–111 users/month.

Two narrowing toggles deliberately NOT applied (each a one-line `WHERE` + a 3-batch re-refresh):
1. **Self-funding** — exclude deposits where `counterparty` = the wallet's own GA owner.
2. **Temporal** — exclude deposits with `block_timestamp` before the wallet became GA-owned
   (`first_ga_owner_at`), relevant for imported wallets.
