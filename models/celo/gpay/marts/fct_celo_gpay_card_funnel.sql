{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='safe_address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay']
  )
}}

-- One row per Celo GP card: when it was issued, when it first received money, when it
-- first spent, and the lags between. The atom behind fct_celo_gpay_funnel_cohorts_monthly.
--
-- The funnel is issued > funded > activated, and those are three DIFFERENT populations
-- that were conflated under one name until 2026-08-05 — see fct_celo_gpay_activity_daily.
--   issued    = a Safe was provisioned as a card (SafeSetup).
--   funded    = it has ever RECEIVED money (any inbound action).
--   activated = it has ever SPENT (a stablecoin settlement to a bridge).
--
-- EVERYTHING IS CLIPPED TO ONE HORIZON, and this is the whole reason the model exists
-- rather than the ratio being computed ad hoc. The three signals come from models that
-- are built at different times: int_celo_gpay_wallets rebuilds in full every run (so its
-- issuance and first-spend reach head), while int_celo_gpay_activity_daily is
-- incremental and lags. On 2026-08-06 wallets was 3 hours old and activity_daily was a
-- day behind it. Left alone, a card issued yesterday would read as issued-but-never-
-- funded purely because funding had not been built yet, and the newest cohort would
-- always look like it converts worst. So `observed_through` is taken from the activity
-- model — the binding constraint — cards issued after it are excluded entirely, and
-- first_spend_at is clipped to it even though wallets knows it. The result is a
-- consistent as-of snapshot rather than a blend of three build times.
--
-- RIGHT-CENSORING IS NOT OPTIONAL HERE. A card issued yesterday has not had time to be
-- funded, and issuance runs at roughly ten cards an hour, so "unfunded" is dominated by
-- cards that are merely young. Dividing funded by issued across all cards therefore
-- measures issuance velocity as much as conversion, and moves whenever issuance
-- accelerates. observation_days is exposed so every rate downstream can be restricted
-- to cards old enough to have had a fair chance; never compute a conversion rate off
-- this table without using it.
--
-- Bounded by card count (1,900 on 2026-08-06), so a full rebuild is trivial and picks up
-- a card whose first spend happens months after issuance without any windowing.

WITH horizon AS (
    -- The furthest date funding has actually been computed to. Deliberately the activity
    -- model and not today(): building this at 09:00 does not mean the world stopped.
    SELECT max(date) AS observed_through
    FROM {{ ref('int_celo_gpay_activity_daily') }}
),

-- First INFLOW per card -> funding. Inbound actions only; the classifier's outbound
-- actions are Payment/Withdrawal/Other. Cashback is currently compiled out of
-- int_celo_gpay_activity and Reversal has never fired, so this is Top-up in practice —
-- naming all three keeps it correct the day either starts.
first_inflow AS (
    SELECT safe_address, min(date) AS first_funded_at
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action IN ('Top-up', 'Reversal', 'Cashback')
    GROUP BY safe_address
),

-- Channel of the card's FIRST inbound transfer (by block_time). Shape classification
-- from int_celo_gpay_funding_tx_envelopes — not an identity label. NULL for unfunded
-- cards. See schema.yml for the channel glossary.
first_fund_channel AS (
    SELECT
        safe_address,
        argMin(funding_channel, block_time) AS first_fund_channel
    FROM {{ ref('int_celo_gpay_funding_tx_envelopes') }}
    GROUP BY safe_address
),

base AS (
    SELECT
        w.safe_address                                   AS safe_address,
        w.issued_at                                      AS issued_at,
        h.observed_through                               AS observed_through,
        -- min(date) over a non-nullable Date column comes back as 1970-01-01 on an
        -- unmatched LEFT JOIN, not NULL, so every unfunded card would read as "funded on
        -- day zero" and produce a nonsense negative lag (docs/lessons/ch-left-join-nulls).
        nullIf(f.first_funded_at, toDate(0))             AS first_funded_at,
        -- Clipped to the same horizon as funding. wallets can see spends that
        -- activity_daily has not been built up to yet; counting them here would produce
        -- cards that are activated but not funded.
        if(w.first_spend_at IS NOT NULL
           AND w.first_spend_at <= h.observed_through,
           w.first_spend_at,
           CAST(NULL AS Nullable(Date)))                 AS first_spend_at,
        -- Empty string on unmatched LEFT JOIN under join_use_nulls=0; nullIf keeps
        -- unfunded cards as NULL rather than ''.
        nullIf(c.first_fund_channel, '')                 AS first_fund_channel
    FROM {{ ref('int_celo_gpay_wallets') }} w
    CROSS JOIN horizon h
    LEFT JOIN first_inflow f ON f.safe_address = w.safe_address
    LEFT JOIN first_fund_channel c ON c.safe_address = w.safe_address
    -- A card issued after the horizon has had zero observation time. It is not an
    -- unfunded card, it is an unobserved one, and including it only drags the newest
    -- cohort down.
    WHERE w.issued_at <= h.observed_through
)

SELECT
    safe_address,
    issued_at,
    first_funded_at,
    first_spend_at,
    first_fund_channel,
    first_funded_at IS NOT NULL                                  AS is_funded,
    first_spend_at  IS NOT NULL                                  AS is_activated,
    -- Days from issuance to each milestone. NULL where the milestone has not happened,
    -- which is distinct from 0 and must stay that way.
    if(first_funded_at IS NOT NULL,
       toInt32(dateDiff('day', issued_at, first_funded_at)),
       CAST(NULL AS Nullable(Int32)))                            AS days_issue_to_fund,
    if(first_spend_at IS NOT NULL,
       toInt32(dateDiff('day', issued_at, first_spend_at)),
       CAST(NULL AS Nullable(Int32)))                            AS days_issue_to_spend,
    -- Only defined for cards that did both. Can legitimately be 0 (funded and spent the
    -- same day, which is the common MiniPay pattern).
    if(first_funded_at IS NOT NULL AND first_spend_at IS NOT NULL,
       toInt32(dateDiff('day', first_funded_at, first_spend_at)),
       CAST(NULL AS Nullable(Int32)))                            AS days_fund_to_spend,
    -- How long this card has been observable. The eligibility gate for every rate.
    toInt32(dateDiff('day', issued_at, observed_through))        AS observation_days,
    observed_through
FROM base
SETTINGS join_use_nulls = 1
