

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_circles_v2_mints_daily`) AS as_of_date
FROM (
-- KPI tile: mints in the last 7 full days vs the prior 7 days.
-- Value = total mint events (count); change_pct vs prior 7d window.

WITH recent AS (
    SELECT
        sum(n_mint_events) AS n_events,
        sum(amount_minted) AS amount
    FROM `dbt`.`int_execution_circles_v2_mints_daily`
    WHERE date >= today() - 7
      AND date <  today()
),
prior AS (
    SELECT
        sum(n_mint_events) AS n_events,
        sum(amount_minted) AS amount
    FROM `dbt`.`int_execution_circles_v2_mints_daily`
    WHERE date >= today() - 14
      AND date <  today() - 7
)

SELECT
    coalesce(r.n_events, 0)                                            AS value,
    coalesce(r.amount,   0.0)                                          AS amount,
    round((toFloat64(r.n_events) - toFloat64(p.n_events))
          / nullIf(toFloat64(p.n_events), 0) * 100, 1)                 AS change_pct
FROM recent r
CROSS JOIN prior p
) AS sub