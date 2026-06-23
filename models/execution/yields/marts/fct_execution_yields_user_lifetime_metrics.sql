{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(wallet_address)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'user_portfolio', 'marts']
    )
}}

WITH

lp_metrics AS (
    SELECT
        provider                         AS wallet_address,
        sum(fees_collected_usd)          AS total_lp_fees_usd,
        countIf(is_active)               AS active_lp_positions,
        countIf(is_active AND is_in_range = true)  AS in_range_positions,
        countIf(is_active AND is_in_range = false) AS out_of_range_positions,
        count()                          AS total_lp_positions,
        min(entry_date)                  AS first_lp_date
    FROM {{ ref('int_execution_yields_user_lp_positions') }}
    GROUP BY wallet_address
),

lending_latest AS (
    SELECT
        user_address                     AS wallet_address,
        sum(balance_usd)                 AS total_lending_balance_usd,
        count()                          AS active_lending_positions,
        min(date)                        AS first_lending_date
    FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
    WHERE date = (
        SELECT max(date) FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}
        WHERE date < today()
    )
      AND balance_usd > 0.01
    GROUP BY wallet_address
),

all_wallets AS (
    SELECT wallet_address FROM lp_metrics
    UNION DISTINCT
    SELECT wallet_address FROM lending_latest
),

joined AS (
    SELECT
        w.wallet_address                                AS wallet_address,
        coalesce(lp.total_lp_fees_usd, 0)               AS total_lp_fees_usd,
        coalesce(ll.total_lending_balance_usd, 0)       AS total_lending_balance_usd,
        coalesce(lp.active_lp_positions, 0)             AS active_lp_positions,
        coalesce(lp.in_range_positions, 0)             AS in_range_positions,
        coalesce(lp.out_of_range_positions, 0)          AS out_of_range_positions,
        coalesce(ll.active_lending_positions, 0)        AS active_lending_positions,
        -- An unmatched LEFT JOIN row yields the type default (epoch 1970-01-01),
        -- NOT NULL, because min(entry_date)/min(date) are non-nullable aggregates.
        -- Convert the epoch sentinel to NULL first (type-correct per column:
        -- first_lp_date is DateTime64(0), first_lending_date is Date) so the
        -- coalesce/least below ignore the missing side instead of returning epoch.
        nullIf(lp.first_lp_date, toDateTime64('1970-01-01 00:00:00', 0))     AS lp_first_date,
        toDateTime64(nullIf(ll.first_lending_date, toDate('1970-01-01')), 0) AS ll_first_date
    FROM all_wallets w
    LEFT JOIN lp_metrics lp      ON lp.wallet_address = w.wallet_address
    LEFT JOIN lending_latest ll  ON ll.wallet_address = w.wallet_address
)

SELECT
    wallet_address,
    total_lp_fees_usd,
    total_lending_balance_usd,
    active_lp_positions,
    in_range_positions,
    out_of_range_positions,
    active_lending_positions,
    -- coalesce(a,b)/coalesce(b,a) keeps least() from ever seeing a NULL: with one
    -- side present both args resolve to that date; with both present it is the min.
    least(
        coalesce(lp_first_date, ll_first_date),
        coalesce(ll_first_date, lp_first_date)
    )                                                AS first_yield_date,
    dateDiff('day',
        least(
            coalesce(lp_first_date, ll_first_date),
            coalesce(ll_first_date, lp_first_date)
        ),
        today()
    )                                                AS tenure_days
FROM joined
