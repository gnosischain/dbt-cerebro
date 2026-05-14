-- Reconciles the income accounting identity at per-validator-per-day grain:
--   balance_gno - balance_prev_gno
--     = consensus_income_amount_gno + deposits_amount_gno - withdrawals_amount_gno
--       + consolidation_inflow_gno - consolidation_outflow_gno
-- Tolerance accommodates Float64 rounding from the Gwei / 1e9 conversion.
-- Returns offending rows; a passing test returns zero rows.

SELECT
    date
    ,validator_index
    ,balance_gno
    ,balance_prev_gno
    ,consensus_income_amount_gno
    ,deposits_amount_gno
    ,withdrawals_amount_gno
    ,consolidation_inflow_gno
    ,consolidation_outflow_gno
    ,(balance_gno - balance_prev_gno)
     - (consensus_income_amount_gno + deposits_amount_gno - withdrawals_amount_gno
        + consolidation_inflow_gno - consolidation_outflow_gno) AS residual
FROM {{ ref('int_consensus_validators_income_daily') }}
WHERE
    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(date) >= today() - {{ var('test_lookback_days', 7) }}
    {% endif %}
    AND ABS(
        (balance_gno - balance_prev_gno)
        - (consensus_income_amount_gno + deposits_amount_gno - withdrawals_amount_gno
           + consolidation_inflow_gno - consolidation_outflow_gno)
    ) > 1e-6
