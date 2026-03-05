

SELECT 'native' AS unit, value, change_pct
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'CashbackGNO' AND window = '7D'

UNION ALL

SELECT 'usd' AS unit, value, change_pct
FROM `dbt`.`fct_execution_gpay_snapshots`
WHERE label = 'CashbackVolume' AND window = '7D'