# Economic concepts & definitions across domains

Canonical definitions for **user**, **active**, **economically active**, **action**, **funded**,
**conversion**, and **referral** as modelled in dbt-cerebro, with per-domain bindings and the known
divergences. Domains: **Gnosis App (GA)**, **Gnosis Pay (GP)**, **Circles v2**, **Mixpanel/UTM**.

## The scoping rule: in-app vs ecosystem

The single reusable predicate for "action taken in the Gnosis App":

```
tx.to_address  = EntryPoint v0.7 (0x…71727de22e5e9d8baf0edac6f37da032)
tx.from_address ∈ seeds/gnosis_app_relayers.csv (is_active = 1)
tx.block_timestamp >= 2025-11-12 (Cometh v4 launch = var gnosis_app_wau_floor_date)
```

Circles is an open protocol: registrations, trusts, mints and invitation fees can be produced by
**any** app or direct on-chain calls. Metrics therefore come in two scopes, and every model states
which one it is:
- **ecosystem** — all Circles activity regardless of origin (the right lens for Circles
  Garage / builders programs);
- **in-app** — filtered by the predicate above and/or by GA-user membership
  (`int_execution_gnosis_app_users_current`).

## User (identity grain)

| Domain | Grain | Model |
|---|---|---|
| Gnosis App | EOA `address` → `user_pseudonym` (monolithic; no roles) | `int_execution_gnosis_app_user_identity_bridge` |
| Gnosis Pay | `(user_pseudonym, identity_role ∈ {initial_owner, delegate, safe_self}, gp_safe)` — pick the persona at query time; **delegate ≠ owner** | `int_execution_gpay_user_identity_bridge` |
| Circles | `avatar` (Human / Group / Org), `invited_by` for Humans | `int_execution_circles_v2_avatars` |
| Mixpanel | `user_id_hash` (= `pseudonymize_address(distinct_id)`; same salted hash space as wallet pseudonyms — that equality is the wallet↔Mixpanel join) | `int_mixpanel_ga_user_acquisition` |

**App↔GP link** (one-way, GA→GP): `int_execution_gnosis_app_gpay_wallets` — `pay_wallet` →
`first_ga_owner_address` (the human GA controller enabled on the Safe's Delay module). Needed
because app-onboarded Safes' `initial_owner` is the Cometh relayer, not the human; UTM attribution
follows the GA controller (GA-controller-wins coalesce in `int_mixpanel_ga_gpay_first_events`).

## Active

Gnosis App **DAU / WAU / MAU are ONE consistent family** — same population
(Gnosis-App-only: a tx through the app's Cometh relayer, or a GA-specific action
— top-up, marketplace, in-app swap, in-app Circles register/trust/mint), same
columns (`active/new/returning/reactivated/cumulative`), no floor, no blacklist
— differing only by time grain. This makes **WAU/DAU** (weekly stickiness, ~1.1–1.5)
and **DAU/MAU** (~0.25–0.4) valid ratios.

| Metric | Definition | Scope | Model | Endpoints |
|---|---|---|---|---|
| **GA DAU** | distinct addresses/day with GA activity (`activity_kind != onboard`) | in-app | `fct_execution_gnosis_app_users_daily` | `api:gnosis_app_users` (daily) · `api:gnosis_app_kpi_dau` |
| **GA WAU** | same, weekly | in-app | `fct_execution_gnosis_app_users_weekly` | `api:gnosis_app_users` (weekly) · `api:gnosis_app_kpi_weekly_active_users` |
| **GA MAU** | same, monthly | in-app | `fct_execution_gnosis_app_users_monthly` | `api:gnosis_app_users` (monthly) · `api:gnosis_app_kpi_mau` |
| **Circles ecosystem weekly active** — separate, **NOT app growth** | GA activity **+ the whole Circles network's** active avatars (register/trust/mint via any wallet) + Cometh swaps; 2025-11-12 floor + blacklist | ecosystem | `fct_execution_gnosis_app_weekly_active_users_circles_ecosystem` | `api:gnosis_app_circles_ecosystem_weekly_active_users` |
| **GP active users** (`api:gpay_active_users`) | distinct wallets with ≥ 1 card **Payment** — Payment-only (deposits/cashback don't count) | GP accounts | `fct_execution_gpay_activity_weekly` | |

Do **not** confuse the Circles-ecosystem reach number with Gnosis App growth — it
counts Circles activity through ANY app/wallet (10–57% of it never opened the
Gnosis App, and rising). GP "active" is Payment-only and **not comparable** to GA
active — don't sum or ratio across products.

## Economically active (circles-first layering)

```
Circles layer (ecosystem, NO app filter)
  int_execution_circles_v2_economically_active_avatars_weekly
    avatar earned >= 1 gCRC cashback OR >= 1 CRC inviter fee that week
    any_in_app_tx flags fee events that came via a GA relayer tx
    → fct/api_execution_circles_v2_economically_active_avatars_weekly

Gnosis App layer (filtered downstream)
  int_execution_gnosis_app_weekly_earners
    = circles layer ∩ GA users, AND inviter fees must be in-app (any_in_app_tx=1)
  WEAU = (in-app GA WAU) ∩ earners → api:gnosis_app_weekly_economically_active_users
    weekly-only; subset of the headline GA WAU, so WEAU/WAU is an activation rate
```

The inviter-fee heuristic (wrapped-CRC transfer invitee→inviter in the **same tx** as a personal
mint, `int_execution_circles_v2_inviter_fees`) is ecosystem-wide by design — another app
implementing invitation fees produces rows, distinguished by `is_gnosis_app_tx`. GP has **no**
economically-active concept (cashback exists but isn't folded into an activity metric).

## Action taxonomies (NOT unified — mapping only)

| Real-world event | GA name | GP name | Circles name |
|---|---|---|---|
| card payment | — | `Payment` (`gp.payment`) | — |
| fund the card | `topup` (initiated in-app) | `Fiat Top Up` / `Crypto Deposit` (`gp.deposit`) | — |
| swap | `swap_signed` / `swap_filled` | — | — |
| cashback | — | `Cashback` (`gp.cashback_claim`) | gCRC cashback (different program!) |
| mint CRC | `circles_personal_mint` (in-app) | — | personal mint (ecosystem) |
| trust | `circles_trust` (in-app) | — | trust update (ecosystem) |
| invite/refer | `circles_invite_human` (in-app) | — | `invited_by` registration (ecosystem) |
| marketplace | `marketplace_buy` | — | — |
| token offer | `token_offer_claim` | — | — |

GP's `Cashback` (GNO, from the GP cashback wallet) and Circles' gCRC cashback are **different
programs** — never union them.

## Funded / conversions / funnel

- **funded** (canonical, GP): `gpay_funded` = first inflow (Fiat Top Up | Crypto Deposit) per Safe
  (`int_execution_gpay_conversions`). GA's closest signal is first `topup` (in-app card funding).
- **GA conversions**: `topup`, `swap_filled`, `marketplace_buy`, `token_offer_claim`,
  + **`starts_referring`** (below).
- **starts_referring** (on-chain referral): first time an address appears as `invited_by` on a new
  Human registration (`int_execution_circles_v2_referrers`; `first_inviter_fee_at` = the stricter
  paid-referral milestone; `first_referral_in_app` = invitee registered in-app).

## Growth funnel by UTM (Mixpanel, GDPR-safe)

Per-user joins happen **only inside the warehouse** at pseudonym grain (`internal_only`,
`expose_to_mcp: false`); everything exposed is **aggregate-only** with a **k-anonymity floor of 5**
(small campaign cohorts suppressed/bucketed). Mixpanel models are blanket-excluded from cerebro-api
(`dbt_project.yml`); Mixpanel data is never pushed to Dune.

| Step | Source | Weekly model |
|---|---|---|
| signup | first identified Mixpanel appearance | `fct_mixpanel_ga_campaign_funnel_weekly` |
| card_ordered / circles_created / crc_minted | Mixpanel client events (seed `mixpanel_conversion_events.csv`) | `fct_mixpanel_ga_client_conversions_weekly` |
| funded / first_payment | on-chain GP conversions, UTM via GA-controller | `fct_mixpanel_ga_gpay_acquisition_weekly` |
| starts_referring | on-chain Circles referrers | `fct_mixpanel_ga_gnosis_app_acquisition_weekly` |
| retention per campaign | funded cohorts × weekly Payments | `fct_mixpanel_ga_gpay_campaign_retention_weekly` |
| engagement/value per campaign | payments, USD volume, cashback | `fct_mixpanel_ga_gpay_campaign_metrics_weekly` |

All keyed by `utm_campaign + utm_source + utm_medium` × `attribution_model`
(first_touch | last_touch; cohorts/funnel use first-touch).

## Known divergences (documented, intentionally not changed)

1. GA WAU multi-source vs GP active Payment-only (different questions; don't compare).
2. GP identity roles vs GA monolithic identity (a delegate's payment attributes to the delegate
   pseudonym, not the owner — pick `identity_role` deliberately).
3. App↔GP link is one-way (GA→GP); GP models do not carry the GA owner.
4. Action names are domain-local (mapping table above); no global enum.
5. GA has no `funded` milestone (use GP's `gpay_funded`); GP has no economically-active metric.
