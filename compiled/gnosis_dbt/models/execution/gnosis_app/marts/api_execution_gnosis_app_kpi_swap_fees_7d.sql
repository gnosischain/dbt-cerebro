

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily`) AS as_of_date
FROM (
-- KPI: protocol fee revenue from filled swaps in the last 7 full days.

WITH recent AS (
    SELECT
        sum(fee_usd_total) AS recent_fee_usd,
        sum(volume_usd)    AS recent_volume_usd
    FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily`
    WHERE date >= today() - 7
      AND date <  today()
),
prior AS (
    SELECT
        sum(fee_usd_total) AS prior_fee_usd
    FROM `dbt`.`int_execution_gnosis_app_swap_fees_daily`
    WHERE date >= today() - 14
      AND date <  today() - 7
)

SELECT
    round(coalesce(r.recent_fee_usd, 0), 2)                                          AS value,
    round(coalesce(r.recent_volume_usd, 0), 2)                                       AS volume_usd,
    round((r.recent_fee_usd - p.prior_fee_usd) / nullIf(p.prior_fee_usd, 0) * 100, 1) AS change_pct
FROM recent r
CROSS JOIN prior p
) AS sub