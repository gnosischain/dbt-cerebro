{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(withdrawal_credentials, date)',
        unique_key='(date, withdrawal_credentials)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "validators_apy"]
    )
}}

-- Per-(date, withdrawal_credentials) cross-sectional APY distribution across the validators
-- sharing a credential, plus rolling 7-day / 30-day medians of the credential-level
-- balance-weighted APY time series. Two kinds of smoothing coexist here:
--
--   1. CROSS-SECTIONAL QUANTILES (q05_apy..q95_apy): on each (date, credential), T-Digest
--      quantiles across the N validators in that credential's set — meaningful when N > 1,
--      collapses to a single point when N == 1 (solo credentials).
--
--   2. TIME-SERIES ROLLING MEDIAN (apy_rolling_7d_median, apy_rolling_30d_median): on the
--      credential's own daily weighted-mean APY series, median over the trailing 7 / 30
--      days. Meaningful even for N == 1 — this is the primary line the dashboard plots
--      for solo credentials where the raw daily APY is jittery.
--
-- Source: int_consensus_validators_income_daily INNER JOIN fct_consensus_validators_status_latest
-- for credential assignment, with the same exit-date guard used in
-- fct_consensus_validators_explorer_daily so an exited validator doesn't keep contributing
-- zero-APY rows after its exit.
--
-- Incremental strategy: monthly windows via apply_monthly_incremental_filter. Rolling
-- windows need a lookback extended by at least 30 days to seed the trailing quantile
-- window correctly at each partition boundary — we pull the full month + 30d of prior
-- data so the rolling median on day 1 of a new month is correct.

WITH time_helpers AS (
    SELECT genesis_time_unix, seconds_per_slot, slots_per_epoch
    FROM {{ ref('stg_consensus__time_helpers') }}
    LIMIT 1
),

-- Active (date, validator_index) universe for the incremental window, extended by 30 days
-- of lookback so the rolling median at the start of the window is correct.
filtered AS (
    SELECT
        toStartOfDay(i.date) AS date
        ,wl.withdrawal_credentials AS withdrawal_credentials
        ,i.validator_index AS validator_index
        ,i.apy AS apy
        ,i.balance_prev_gno AS balance_prev_gno
    FROM {{ ref('int_consensus_validators_income_daily') }} i
    INNER JOIN {{ ref('fct_consensus_validators_status_latest') }} wl
        ON wl.validator_index = i.validator_index
    CROSS JOIN time_helpers th
    WHERE i.apy > 0 AND i.apy < 200
      AND i.effective_balance_gno > 0
      AND (
          wl.exit_epoch >= toUInt64(18446744073709551615)
          OR i.date <= toDate(toDateTime(th.genesis_time_unix + wl.exit_epoch * th.slots_per_epoch * th.seconds_per_slot))
      )
      {% if start_month and end_month %}
        AND toStartOfMonth(i.date) >= addMonths(toDate('{{ start_month }}'), -1)
        AND toStartOfMonth(i.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('i.date', 'date', 'true', lookback_days=30) }}
      {% endif %}
),

-- Cross-sectional quantiles + balance-weighted mean, per (date, credential).
daily_agg AS (
    SELECT
        date
        ,withdrawal_credentials
        ,quantilesTDigest(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)(apy) AS q_apy
        ,IF(
            SUMIf(balance_prev_gno, balance_prev_gno > 0) > 0,
            SUMIf(apy * balance_prev_gno, balance_prev_gno > 0)
              / SUMIf(balance_prev_gno, balance_prev_gno > 0),
            0
        ) AS apy_weighted
        ,uniqExact(validator_index) AS validator_count_active
    FROM filtered
    GROUP BY date, withdrawal_credentials
)

SELECT
    date
    ,withdrawal_credentials
    ,q_apy[1]  AS q05_apy
    ,q_apy[2]  AS q10_apy
    ,q_apy[3]  AS q25_apy
    ,q_apy[4]  AS q50_apy
    ,q_apy[5]  AS q75_apy
    ,q_apy[6]  AS q90_apy
    ,q_apy[7]  AS q95_apy
    ,apy_weighted
    ,validator_count_active
    ,quantile(0.5)(apy_weighted) OVER (
        PARTITION BY withdrawal_credentials
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS apy_rolling_7d_median
    ,quantile(0.5)(apy_weighted) OVER (
        PARTITION BY withdrawal_credentials
        ORDER BY date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS apy_rolling_30d_median
FROM daily_agg
{% if start_month and end_month %}
-- Trim the 30-day lookback so the written partition only contains the requested month.
WHERE toStartOfMonth(date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(date) <= toDate('{{ end_month }}')
{% endif %}
