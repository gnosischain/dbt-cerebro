{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:stablecoin_holder_cohorts', 'granularity:quarterly'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "parameters": [
                    {"name": "quarter_from", "column": "quarter", "operator": ">=", "type": "date", "description": "Inclusive lower bound on quarter start date (e.g. 2024-01-01 for 2024-Q1)"},
                    {"name": "quarter_to", "column": "quarter", "operator": "<=", "type": "date", "description": "Inclusive upper bound on quarter start date"},
                    {"name": "peg_class", "column": "peg_class", "operator": "=", "type": "string", "description": "Filter by peg class: USD-pegged or non-USD"}
                ],
                "pagination": {"enabled": true, "default_limit": 200, "max_limit": 1000, "response": "envelope"},
                "sort": [{"column": "quarter", "direction": "DESC"}]
            }
        }
    )
}}

SELECT
    quarter,
    peg_class,
    balance_bucket,
    holders_min,
    holders_max,
    holders_avg,
    holders_median,
    value_min,
    value_max,
    value_avg,
    value_median,
    value_median / nullIf(holders_median, 0) AS avg_balance_usd
FROM {{ ref('int_quarterly_stablecoin_cohorts_stats') }}
ORDER BY quarter, peg_class, bucket_order
