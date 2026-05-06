{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:stablecoin_supply', 'granularity:quarterly'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "quarter_from", "column": "quarter", "operator": ">=", "type": "date", "description": "Inclusive lower bound on quarter start date (e.g. 2024-01-01 for 2024-Q1)"},
                    {"name": "quarter_to", "column": "quarter", "operator": "<=", "type": "date", "description": "Inclusive upper bound on quarter start date"}
                ],
                "pagination": {"enabled": true, "default_limit": 200, "max_limit": 1000, "response": "envelope"},
                "sort": [{"column": "quarter", "direction": "DESC"}]
            }
        }
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    sum(supply_usd) AS supply_usd,
    sum(holders)    AS holders_total
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE token_class = 'STABLECOIN'
  AND date = toDate(subtractDays(addQuarters(toStartOfQuarter(date), 1), 1))
GROUP BY quarter
ORDER BY quarter
