

-- Network-level wrapped vs unwrapped share over time.
--   wrapped_supply   = cumulative ERC-20 wrapped CRC across all wrappers
--   unwrapped_supply = total_supply - wrapped_supply
--   wrapped_pct      = wrapped_supply / total_supply
--
-- Builds on int_execution_circles_v2_wrapper_supply_daily for the wrapped
-- side and fct_execution_circles_v2_total_supply_daily for the denominator.

WITH wrapper_daily AS (
    SELECT
        date,
        sum(supply_delta) AS delta_today
    FROM `dbt`.`int_execution_circles_v2_wrapper_supply_daily`
    GROUP BY date
),
wrapped_cum AS (
    SELECT
        date,
        sum(delta_today) OVER (
            ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS wrapped_supply
    FROM wrapper_daily
),
total AS (
    SELECT date, total_supply
    FROM `dbt`.`fct_execution_circles_v2_total_supply_daily`
)

SELECT
    t.date                                                                AS date,
    coalesce(w.wrapped_supply, 0)                                         AS wrapped_supply,
    t.total_supply - coalesce(w.wrapped_supply, 0)                        AS unwrapped_supply,
    t.total_supply                                                        AS total_supply,
    round(coalesce(w.wrapped_supply, 0) / nullIf(t.total_supply, 0) * 100, 2) AS wrapped_pct
FROM total t
LEFT JOIN wrapped_cum w ON w.date = t.date
WHERE t.date < today()
ORDER BY t.date DESC