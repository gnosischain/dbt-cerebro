

SELECT sub.*, (SELECT toDate(max(date)) FROM `dbt`.`int_bridges_flows_daily`) AS as_of_date
FROM (
SELECT
  round(vol_7d, 2)        AS value,
  round(vol_prev_7d, 2)   AS prev_value,
  chg_vol_7d              AS change_pct
FROM `dbt`.`fct_bridges_kpis_snapshot`
ORDER BY as_of_date DESC
LIMIT 1
) AS sub