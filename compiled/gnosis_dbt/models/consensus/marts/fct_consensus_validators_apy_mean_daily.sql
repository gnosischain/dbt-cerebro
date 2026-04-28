

-- Materialized as a physical table so the dashboard API returns in milliseconds.
-- Running balance-weighted SUMIf over the 200M-row income fact would 30s-timeout
-- Vercel functions. Output is 1568 rows, trivial to rebuild whenever the upstream
-- int_consensus_validators_income_daily updates.

-- Network-wide daily mean APY. Weighted by balance_prev_gno so exited/idle/just-entered
-- validators (which have apy=0 and/or tiny balance) don't drag the average down to
-- meaninglessly low values. This aligns the mean with the median of the APY Distribution
-- chart, which also effectively filters those validators via its quantile window.
--
-- Outlier filter matches int_consensus_validators_dists_daily: apy in (0, 200).
-- Zero-APY validators are dropped (they include exited validators that contributed
-- nothing but were once active, as well as validators still in the entry queue).
--
-- v2 additions (2026-04):
--   * apy_rolling_7d_median / _30d_median — trailing medians of the daily weighted
--     mean, used as a smoothing overlay on the dashboard band chart.

WITH base AS (
    SELECT
        date
        ,SUMIf(apy * balance_prev_gno, apy > 0 AND apy < 200 AND balance_prev_gno > 0)
          / NULLIF(SUMIf(balance_prev_gno, apy > 0 AND apy < 200 AND balance_prev_gno > 0), 0) AS apy
    FROM `dbt`.`int_consensus_validators_income_daily`
    GROUP BY date
)

SELECT
    date
    ,apy
    ,quantile(0.5)(apy) OVER (
        ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS apy_rolling_7d_median
    ,quantile(0.5)(apy) OVER (
        ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS apy_rolling_30d_median
FROM base
ORDER BY date