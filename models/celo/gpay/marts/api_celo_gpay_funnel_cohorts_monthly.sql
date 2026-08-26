{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funnel_cohorts', 'granularity:monthly'],
    meta={
      "api": {
        "methods": ["GET"],
        "allow_unfiltered": true,
        "parameters": [
          {"name": "start_date", "column": "month", "operator": ">=", "type": "date", "description": "Inclusive start of issuance cohort month"},
          {"name": "end_date", "column": "month", "operator": "<=", "type": "date", "description": "Inclusive end of issuance cohort month"}
        ],
        "sort": [{"column": "month", "direction": "DESC"}]
      }
    }
  )
}}

-- Card funnel by issuance cohort. `month` is the month the cards were ISSUED, not a
-- month of activity — a row's figures describe what that group of cards went on to do,
-- so the series does not read left-to-right as a timeline.
--
-- The windowed rates are the comparable ones. funded_ever/activated_ever slope down
-- toward the present because recent cohorts have had less time, not because they convert
-- worse; cohort_complete_30d flags rows whose 30-day figures are still provisional. See
-- fct_celo_gpay_funnel_cohorts_monthly for the full reasoning.
SELECT
    toString(cohort_month)   AS month,
    cards_issued,
    funded_ever,
    activated_ever,
    funded_rate_7d,
    funded_rate_30d,
    activated_rate_7d,
    activated_rate_30d,
    spend_rate_of_funded_30d,
    median_days_issue_to_fund,
    median_days_fund_to_spend,
    cohort_complete_7d,
    cohort_complete_30d
FROM {{ ref('fct_celo_gpay_funnel_cohorts_monthly') }}
ORDER BY month
