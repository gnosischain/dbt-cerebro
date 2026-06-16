{{
    config(
        materialized='view',
        tags=['production', 'execution', 'pools', 'trades', 'api']
    )
}}

SELECT
    date,
    aggregator_label    AS label,
    share_pct           AS value
FROM {{ ref('fct_execution_trades_by_aggregator_daily') }}
WHERE date < today()
ORDER BY date, label
