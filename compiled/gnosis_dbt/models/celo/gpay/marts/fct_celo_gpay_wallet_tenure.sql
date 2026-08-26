






-- One row per GP card funding wallet: how long it had been active on Celo before it
-- funded a card. The acquisition-vs-cross-sell question, answered per wallet.
--
-- WHAT IT SETTLES. If wallets predate their card by months, GP on Celo is selling into
-- MiniPay's installed base — a distribution result. If they appear days before the card,
-- GP is genuinely bringing people on-chain — an acquisition result. Both produce the
-- same headline card count, and until this model existed the two were indistinguishable.
--
-- LEFT-CENSORING IS THE CENTRAL CAVEAT AND IT IS NOT A ROUNDING ERROR. celo_execution
-- starts at the L2 migration (2025-03-26); Celo's L1 history is not in this
-- warehouse. So `first_seen_at` means "first seen BY US", and for any wallet already
-- active at the migration it is a floor, not a birth date. Those wallets pile up at the
-- window's edge and would otherwise manufacture a fake cohort of "wallets born the day
-- the L2 launched". is_left_censored marks them; days_before_first_funding is a LOWER
-- BOUND for them and exact for everyone else. Never average the column without
-- splitting on the flag, and never report a median tenure over a population where the
-- censored share is material — report the censored share alongside it instead.
--
-- Fee legs count as presence here (see int_celo_gpay_funder_wallet_monthly_presence):
-- a gas receipt is not economic activity but it is proof the wallet transacted. The
-- stricter reading is available via first_active_month_nonfee.
--
-- Bounded by wallet count (~1k), so a full rebuild is trivial — all the cost sits in the
-- monthly presence model this reads.

WITH presence AS (
    SELECT
        wallet_address                                            AS wallet_address,
        min(first_seen_at)                                        AS first_seen_at,
        max(last_seen_at)                                         AS last_seen_at,
        uniqExact(month)                                          AS months_present,
        sum(n_legs)                                               AS n_legs_lifetime,
        sum(n_legs_nonfee)                                        AS n_legs_nonfee_lifetime,
        minIf(month, n_legs_nonfee > 0)                           AS first_active_month_nonfee
    FROM `dbt`.`int_celo_gpay_funder_wallet_monthly_presence`
    GROUP BY wallet_address
),

joined AS (
    SELECT
        w.wallet_address                                          AS wallet_address,
        w.card_safe_address                                       AS card_safe_address,
        w.is_solo_funder                                          AS is_solo_funder,
        w.n_cards_funded                                          AS n_cards_funded,
        toDate(w.first_funded_at)                                 AS first_funded_at,
        -- A wallet present in the funder spine but absent from presence would be a
        -- coverage bug, not a real state: it funded a card, so it made a Transfer.
        -- nullIf keeps that visible as NULL rather than as 1970 (join_use_nulls is off
        -- by default here — docs/lessons/ch-left-join-nulls.md).
        nullIf(toDate(p.first_seen_at), toDate(0))                AS first_seen_at,
        nullIf(toDate(p.last_seen_at), toDate(0))                 AS last_seen_at,
        nullIf(p.first_active_month_nonfee, toDate(0))            AS first_active_month_nonfee,
        p.months_present                                          AS months_present,
        p.n_legs_lifetime                                         AS n_legs_lifetime,
        p.n_legs_nonfee_lifetime                                  AS n_legs_nonfee_lifetime
    FROM `dbt`.`int_celo_gpay_funder_wallets` w
    LEFT JOIN presence p ON p.wallet_address = w.wallet_address
),

measured AS (
    SELECT
        *,
        -- Non-negative by construction: the card-funding transfer is itself a leg this
        -- wallet sent, so presence always sees the wallet at or before its first funding.
        if(first_seen_at IS NULL, CAST(NULL AS Nullable(Int32)),
           toInt32(dateDiff('day', first_seen_at, first_funded_at)))  AS days_before_first_funding,
        toUInt8(first_seen_at IS NOT NULL
                AND first_seen_at < toDate('2025-03-26')
                                   + 7)      AS is_left_censored
    FROM joined
)

SELECT
    wallet_address,
    card_safe_address,
    is_solo_funder,
    n_cards_funded,
    first_seen_at,
    last_seen_at,
    first_active_month_nonfee,
    first_funded_at,
    months_present,
    n_legs_lifetime,
    n_legs_nonfee_lifetime,
    days_before_first_funding,
    is_left_censored,
    -- Bucketed for reporting. The censored bucket is FIRST in precedence: for those
    -- wallets the day count is a floor, so filing them under whatever bucket that floor
    -- happens to land in would understate tenure and quietly bias every cohort built on
    -- this column. They get their own label instead of a wrong one.
    multiIf(
        days_before_first_funding IS NULL,          'unknown',
        is_left_censored = 1,                       'pre_dates_our_window',
        days_before_first_funding = 0,              'same_day',
        days_before_first_funding <= 7,             '1_7_days',
        days_before_first_funding <= 30,            '8_30_days',
        days_before_first_funding <= 90,            '31_90_days',
        days_before_first_funding <= 180,           '91_180_days',
                                                    'over_180_days'
    )                                                                 AS tenure_bucket,
    -- The blunt read for a headline: was this wallet doing things on Celo well before
    -- the card? Censored wallets are TRUE by definition — they predate our window.
    toUInt8(is_left_censored = 1
            OR (days_before_first_funding IS NOT NULL
                AND days_before_first_funding >= 30))                 AS is_pre_existing_user
FROM measured
SETTINGS join_use_nulls = 1