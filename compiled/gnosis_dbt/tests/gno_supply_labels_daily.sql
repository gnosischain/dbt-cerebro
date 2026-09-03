-- Every day in the GNO-supply endpoint must carry exactly the 4 label rows.
-- Downstream consumers assume a dense 4-row daily grain: governance turnout
-- joins supply by exact date (no ASOF fallback) and the dashboard stacks all
-- labels per day, so a day with fewer labels silently zeroes a series instead
-- of failing. Returns offending days; passing = zero rows. Yesterday is
-- excluded because the feed legitimately lags to D-1.
-- NOTE: three labels are mutually exclusive components (Ethereum Circ., Gnosis
-- Circ., Non-Circ.); 'Total Circ. Supply' is DERIVED (Ethereum Circ. + Gnosis
-- Circ.). Summing or stacking all four double-counts the circulating portion —
-- consumers must filter by label.
SELECT
    date AS d
    ,count() AS label_rows
    ,uniqExact(label) AS labels
FROM `dbt`.`api_gno_supply_daily`
WHERE date >= today() - 7
  AND date < today() - 1
GROUP BY date
HAVING label_rows != 4 OR labels != 4