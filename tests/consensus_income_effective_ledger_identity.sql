-- Per-(date, validator_index) ledger identity using EFFECTIVE deposits credited (the
-- spec-bounded credit produced by int_consensus_validators_income_daily), not the raw
-- reported deposit amount.
--
--   balance_gno - balance_prev_gno
--     = consensus_income_amount_gno
--       + effective_deposits_credited_gno
--       - withdrawals_amount_gno
--       + consolidation_inflow_gno
--       - consolidation_outflow_gno
--
-- This is the identity the v2 spec-cap algorithm enforces by construction. A non-zero
-- residual means either the model's internal math broke or an upstream dataset (usually
-- int_consensus_validators_consolidations_daily after the v4 ReplacingMergeTree fix)
-- drifted from its invariants.
--
-- Returns offending rows; passing = zero rows. Tolerance 1e-6 GNO for Float64 rounding.
SELECT
    date
    ,validator_index
    ,balance_gno
    ,balance_prev_gno
    ,consensus_income_amount_gno
    ,effective_deposits_credited_gno
    ,withdrawals_amount_gno
    ,consolidation_inflow_gno
    ,consolidation_outflow_gno
    ,(balance_gno - balance_prev_gno)
     - (consensus_income_amount_gno + effective_deposits_credited_gno
        - withdrawals_amount_gno
        + consolidation_inflow_gno - consolidation_outflow_gno) AS residual
FROM {{ ref('int_consensus_validators_income_daily') }}
WHERE
    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(date) >= today() - {{ var('test_lookback_days', 7) }}
    {% endif %}
    AND ABS(
        (balance_gno - balance_prev_gno)
        - (consensus_income_amount_gno + effective_deposits_credited_gno
           - withdrawals_amount_gno
           + consolidation_inflow_gno - consolidation_outflow_gno)
    ) > 1e-6
