{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='safe_address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','funder_wallet']
  )
}}

{% set alive_window_days = 30 %}
{% set large_transfer_usd = 1000 %}  {# same threshold as fct_celo_gpay_wallet_share_of_spend_daily #}

-- One row per Celo GP card, joining the card's own lifecycle to whether the person
-- behind it is still economically alive on Celo at all.
--
-- THE QUESTION THIS ANSWERS. The card-side tree can tell you a card stopped being used.
-- It cannot tell you WHY, and the two possible whys need opposite responses: a holder
-- who left the chain entirely is lost, while a holder still transacting daily in their
-- own wallet has simply stopped choosing the card — that one is winnable, and until
-- this model existed both looked identical (a churned card).
--
-- Measured 2026-08-10 over 2026-08-03..10, the split was not close: of cards dormant
-- 7-30 days, 74% of holders were still active in-wallet; dormant 30+, 65%; funded but
-- never spent, 83%. That is a reactivation population larger than the active card base.
--
-- CLIPPED TO ONE HORIZON, for the same reason fct_celo_gpay_card_funnel is. The card
-- signal (int_celo_gpay_activity_daily) and the wallet signal
-- (int_celo_gpay_funder_wallet_daily) are separate incremental models built at
-- different times. Whichever is further behind is the binding constraint: if the wallet
-- model lags by a day, every holder reads as newly silent and the whole table swings
-- toward "gone". observed_through is the LEAST of the two, never today().
--
-- MAPPING IS PARTIAL AND SAYS SO. Only solo funders (one wallet, one card) can be
-- attributed unambiguously. Cards funded through a hub or a contract router get
-- wallet_address = NULL and land in a '__wallet_unmapped' segment — an honest third
-- category, not a silent drop and not a guess. Never read unmapped as "gone".
--
-- Bounded by card count, so a full rebuild is trivial.

WITH horizon AS (
    SELECT least(
        (SELECT max(date) FROM {{ ref('int_celo_gpay_activity_daily') }}),
        (SELECT max(date) FROM {{ ref('int_celo_gpay_funder_wallet_daily') }})
    ) AS observed_through
),

first_inflow AS (
    SELECT safe_address, min(date) AS first_funded_at
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action IN ('Top-up', 'Reversal', 'Cashback')
    GROUP BY safe_address
),

last_payment AS (
    SELECT safe_address, max(date) AS last_payment_at
    FROM {{ ref('int_celo_gpay_activity_daily') }}
    WHERE action = 'Payment'
    GROUP BY safe_address
),

-- Solo funders only: a hub funder's history belongs to no single cardholder.
wallet_map AS (
    SELECT card_safe_address AS safe_address, wallet_address
    FROM {{ ref('int_celo_gpay_funder_wallets') }}
    WHERE is_solo_funder = 1 AND card_safe_address IS NOT NULL
),

wallet_last_active AS (
    SELECT wallet_address, max(date) AS wallet_last_active_at
    FROM {{ ref('int_celo_gpay_funder_wallet_daily') }}
    GROUP BY wallet_address
),

-- Recent-window wallet economics, for sizing the opportunity rather than just counting
-- heads. Outbound only: what the holder is spending, and how much of it the card got.
--
-- Reads the per-transfer base rather than the daily fold specifically so the large-
-- transfer split is available. Without it these columns are a trap: summed raw, a
-- handful of treasury-scale movements dominate every segment total and a "reactivation
-- prize" figure becomes one whale's transfer (see
-- fct_celo_gpay_wallet_share_of_spend_daily for the worked example — 6 transfers
-- carried 86% of a $365k window). _retail is the number to size an opportunity on;
-- the unsuffixed column stays for flow-of-funds.
wallet_recent AS (
    SELECT
        t.wallet_address                                                   AS wallet_address,
        count()                                                            AS wallet_transfers_recent,
        sumIf(t.amount_usd, t.direction = 'out'
                            AND t.amount_usd IS NOT NULL)                  AS wallet_out_usd_recent,
        sumIf(t.amount_usd, t.direction = 'out'
                            AND t.amount_usd < {{ large_transfer_usd }})   AS wallet_out_usd_recent_retail,
        sumIf(t.amount_usd, t.direction = 'out'
                            AND t.counterparty_class = 'gp_card')          AS wallet_to_card_usd_recent
    FROM {{ ref('int_celo_gpay_funder_wallet_transfers') }} t
    CROSS JOIN horizon h
    WHERE t.block_date > h.observed_through - {{ alive_window_days }}
      -- Gas receipts are not activity; the base still carries them.
      AND t.counterparty_class != 'fee_sink'
    GROUP BY t.wallet_address
),

base AS (
    SELECT
        w.safe_address                                   AS safe_address,
        w.issued_at                                      AS issued_at,
        h.observed_through                               AS observed_through,
        -- min()/max() over a non-nullable Date come back as 1970-01-01 on an unmatched
        -- LEFT JOIN, not NULL (docs/lessons/ch-left-join-nulls.md). Without nullIf every
        -- unfunded card reads as funded on day zero.
        nullIf(f.first_funded_at, toDate(0))             AS first_funded_at,
        nullIf(p.last_payment_at, toDate(0))             AS last_payment_at,
        nullIf(m.wallet_address, '')                     AS wallet_address,
        nullIf(la.wallet_last_active_at, toDate(0))      AS wallet_last_active_at,
        r.wallet_transfers_recent                        AS wallet_transfers_recent,
        r.wallet_out_usd_recent                          AS wallet_out_usd_recent,
        r.wallet_out_usd_recent_retail                   AS wallet_out_usd_recent_retail,
        r.wallet_to_card_usd_recent                      AS wallet_to_card_usd_recent
    FROM {{ ref('int_celo_gpay_wallets') }} w
    CROSS JOIN horizon h
    LEFT JOIN first_inflow       f  ON f.safe_address    = w.safe_address
    LEFT JOIN last_payment       p  ON p.safe_address    = w.safe_address
    LEFT JOIN wallet_map         m  ON m.safe_address    = w.safe_address
    LEFT JOIN wallet_last_active la ON la.wallet_address = m.wallet_address
    LEFT JOIN wallet_recent      r  ON r.wallet_address  = m.wallet_address
    -- A card issued after the horizon is unobserved, not inactive.
    WHERE w.issued_at <= h.observed_through
),

classified AS (
    SELECT
        *,
        multiIf(
            first_funded_at IS NULL,                                          'never_funded',
            last_payment_at IS NULL,                                          'funded_never_paid',
            dateDiff('day', last_payment_at, observed_through) < 7,           'active',
            dateDiff('day', last_payment_at, observed_through) < 30,          'dormant_7_30',
                                                                              'dormant_30plus'
        )                                                                     AS card_state,
        if(wallet_last_active_at IS NULL, CAST(NULL AS Nullable(Int32)),
           toInt32(dateDiff('day', wallet_last_active_at, observed_through))) AS days_since_wallet_active,
        if(last_payment_at IS NULL, CAST(NULL AS Nullable(Int32)),
           toInt32(dateDiff('day', last_payment_at, observed_through)))       AS days_since_last_payment
    FROM base
)

SELECT
    safe_address,
    wallet_address,
    issued_at,
    first_funded_at,
    last_payment_at,
    wallet_last_active_at,
    observed_through,
    card_state,
    days_since_last_payment,
    days_since_wallet_active,
    -- Two horizons on purpose. 30d is what the segment below uses (a month of silence
    -- is a real signal); 7d is the tighter read and the one the 2026-08-10 findings
    -- were quoted on. Chart either, but never mix them in one number.
    toUInt8(days_since_wallet_active IS NOT NULL AND days_since_wallet_active < 7)   AS wallet_active_7d,
    toUInt8(days_since_wallet_active IS NOT NULL
            AND days_since_wallet_active < {{ alive_window_days }})                  AS wallet_active_30d,
    wallet_transfers_recent,
    wallet_out_usd_recent,
    wallet_out_usd_recent_retail,
    wallet_to_card_usd_recent,
    -- The actionable axis: among cards that are NOT currently active, is the holder
    -- still there?
    --
    -- never_funded is deliberately NOT split on the wallet axis. A card that never
    -- received money has no funder, so it can never have a funding wallet — labelling
    -- those 791 cards '__wallet_unmapped' would read as a coverage gap we could close,
    -- when it is true by construction. They are an issuance/activation problem, and the
    -- wallet-side tree has nothing to say about them.
    --
    -- For the rest, '__wallet_unmapped' IS a genuine coverage limit (hub- or
    -- router-funded card, so no single wallet is attributable) and must never be read
    -- as evidence of absence.
    multiIf(
        card_state = 'active',                     'card_active',
        card_state = 'never_funded',               'never_funded',
        wallet_address IS NULL,                    concat(card_state, '__wallet_unmapped'),
        days_since_wallet_active IS NOT NULL
          AND days_since_wallet_active
              < {{ alive_window_days }},           concat(card_state, '__wallet_alive'),
                                                   concat(card_state, '__wallet_gone')
    )                                                                                AS engagement_segment
FROM classified
SETTINGS join_use_nulls = 1
