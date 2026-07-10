-- Reconciles the income accounting identity at per-validator-per-day grain:
--   balance_gno - balance_prev_gno
--     = consensus_income_amount_gno + effective_deposits_credited_gno - withdrawals_amount_gno
--       + consolidation_inflow_gno - consolidation_outflow_gno
--
-- Uses effective_deposits_credited_gno (what actually hit the balance today),
-- NOT deposits_amount_gno (raw REPORTED deposits). income is computed from the
-- effective-credited figure via the spec-cap LEAST/GREATEST, so on deposit-lag
-- days reported != credited and reconciling against the raw column produces a
-- residual equal to that gap (a false failure). The prior version of this test
-- used deposits_amount_gno and only "passed" because the consensus ingestion
-- freeze (2026-06-07) left its 7-day lookback window empty; once data advanced,
-- ~16.5k rows/window flagged, all explained by the reported-vs-credited gap.
-- Tolerance accommodates Float64 rounding from the Gwei / 1e9 / 32 conversion.
-- Returns offending rows; a passing test returns zero rows.

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
     - (consensus_income_amount_gno + effective_deposits_credited_gno - withdrawals_amount_gno
        + consolidation_inflow_gno - consolidation_outflow_gno) AS residual
FROM `dbt`.`int_consensus_validators_income_daily`
WHERE
    toDate(date) >= today() - 7
    
    AND ABS(
        (balance_gno - balance_prev_gno)
        - (consensus_income_amount_gno + effective_deposits_credited_gno - withdrawals_amount_gno
           + consolidation_inflow_gno - consolidation_outflow_gno)
    ) > 1e-6