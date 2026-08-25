-- Every day in the GNO-supply endpoint must carry exactly the 3 label rows.
-- Downstream consumers assume a dense 3-row daily grain: governance turnout
-- joins supply by exact date (no ASOF fallback) and the dashboard stacks all
-- labels per day, so a day with fewer labels silently zeroes a series instead
-- of failing. Returns offending days; passing = zero rows. Yesterday is
-- excluded because the feed legitimately lags to D-1.
-- severity warn until the chain-100 history backfill completes; flip to error
-- at the endpoint switch.
{{ config(severity='warn') }}
SELECT
    date AS d
    ,count() AS label_rows
    ,uniqExact(label) AS labels
FROM {{ ref('api_gno_supply_daily') }}
WHERE date >= today() - {{ var('test_lookback_days', 7) }}
  AND date < today() - 1
GROUP BY date
HAVING label_rows != 3 OR labels != 3
