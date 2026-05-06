{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:stablecoin_transfers', 'granularity:quarterly'],
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
    sum(transfer_count) AS transfers,
    sum(volume_usd)     AS volume_usd
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE token_class = 'STABLECOIN'
GROUP BY quarter
ORDER BY quarter
