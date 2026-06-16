{{
    config(
        materialized='table',
        tags=['production', 'execution', 'pools', 'trades', 'fct']
    )
}}

WITH

daily_totals AS (
    SELECT date, count() AS total
    FROM {{ ref('int_execution_trades_by_tx') }}
    GROUP BY date
),

per_agg AS (
    SELECT
        date,
        aggregator_label,
        count() AS trade_count
    FROM {{ ref('int_execution_trades_by_tx') }}
    GROUP BY date, aggregator_label
)

SELECT
    p.date                                                      AS date,
    p.aggregator_label                                          AS aggregator_label,
    p.trade_count                                               AS trade_count,
    round(100.0 * p.trade_count / d.total, 2)                   AS share_pct
FROM per_agg p
JOIN daily_totals d ON d.date = p.date
