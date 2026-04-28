

-- Materialized as a physical table (not a view) so the dashboard API query
-- returns in milliseconds. Source fact has 200M+ rows; running SUM GROUP BY on
-- every dashboard request would 30s-timeout Vercel functions. Rebuild cost is
-- trivial (1568 output rows) and hooks naturally into the dbt DAG: each time
-- the upstream int_consensus_validators_income_daily runs, this rebuilds too.
--
-- v2 additions (2026-04):
--   * income_gno_rolling_7d_median / _30d_median — advisory overlay lines. Raw
--     income_gno unchanged (real negatives remain visible, per user direction).
--   * validators_snapshot_count — number of validators snapshotted on the date;
--     helps the dashboard annotate any days where an upstream crawler hiccup
--     reduced coverage.
--   * anomaly_flag — True when abs(income_gno) > 5 * trailing-30d median(abs(income_gno))
--     AND abs(income_gno) > 500 GNO. Dashboard surfaces in tooltip; does NOT drop
--     rows. Expected to be rare post-A1 / A2 consolidation fixes.

WITH base AS (
    SELECT
        date
        ,SUM(consensus_income_amount_gno) AS income_gno
    FROM `dbt`.`int_consensus_validators_income_daily`
    GROUP BY date
),

snap_count AS (
    SELECT
        toStartOfDay(date) AS date
        ,count() AS validators_snapshot_count
    FROM `dbt`.`int_consensus_validators_snapshots_daily`
    GROUP BY 1
),

with_rolling AS (
    SELECT
        b.date AS date
        ,b.income_gno AS income_gno
        ,quantile(0.5)(b.income_gno) OVER (
            ORDER BY b.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS income_gno_rolling_7d_median
        ,quantile(0.5)(b.income_gno) OVER (
            ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS income_gno_rolling_30d_median
        ,quantile(0.5)(ABS(b.income_gno)) OVER (
            ORDER BY b.date ROWS BETWEEN 29 PRECEDING AND 1 PRECEDING
        ) AS abs_income_rolling_30d_median_prev
        ,COALESCE(s.validators_snapshot_count, 0) AS validators_snapshot_count
    FROM base b
    LEFT JOIN snap_count s ON s.date = b.date
)

SELECT
    date
    ,income_gno
    ,income_gno_rolling_7d_median
    ,income_gno_rolling_30d_median
    ,validators_snapshot_count
    ,IF(
        ABS(income_gno) > 5 * abs_income_rolling_30d_median_prev
          AND ABS(income_gno) > 500,
        1, 0
    ) AS anomaly_flag
FROM with_rolling
ORDER BY date