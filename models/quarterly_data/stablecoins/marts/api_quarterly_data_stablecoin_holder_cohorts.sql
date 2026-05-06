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

WITH eoq_cohorts AS (
    SELECT
        toStartOfQuarter(date) AS quarter,
        CASE
            WHEN symbol IN ('WxDAI', 'sDAI', 'USDC', 'USDC.e', 'USDT')
            THEN 'USD-pegged'
            ELSE 'non-USD'
        END AS peg_class,
        balance_bucket,
        sum(holders_in_bucket)       AS holders,
        sum(value_usd_in_bucket)     AS value_usd
    FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
    WHERE token_class = 'STABLECOIN'
      AND cohort_unit = 'usd'
      AND date = toDate(subtractDays(addQuarters(toStartOfQuarter(date), 1), 1))
    GROUP BY quarter, peg_class, balance_bucket
)

SELECT
    quarter,
    peg_class,
    balance_bucket,
    holders,
    value_usd,
    value_usd / nullIf(holders, 0) AS avg_balance_usd
FROM eoq_cohorts
ORDER BY
    quarter,
    peg_class,
    multiIf(
        balance_bucket = '0-0.01',    1,
        balance_bucket = '0.01-0.1',  2,
        balance_bucket = '0.1-1',     3,
        balance_bucket = '1-10',      4,
        balance_bucket = '10-100',    5,
        balance_bucket = '100-1k',    6,
        balance_bucket = '1k-10k',    7,
        balance_bucket = '10k-100k',  8,
        balance_bucket = '100k-1M',   9,
        10
    )
