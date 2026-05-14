-- Mass-balance invariant for EIP-7251 cross-consolidations: on each application date,
-- total source outflow must equal total target inflow. 'self' rows carry 0 GNO and do
-- not participate.
--
-- A failure here means either (a) the consolidations model is double-counting, (b)
-- ReplacingMergeTree dedup on the unique key is collapsing target rows (see v4 fix in
-- int_consensus_validators_consolidations_daily.sql) or (c) source_snapshots window
-- missed a source's last-non-zero effective_balance. Any of these corrupts
-- int_consensus_validators_income_daily per-validator income attribution.
--
-- Returns offending rows; passing = zero rows. Tolerance 1e-6 GNO for Float64 rounding.
SELECT
    date
    ,SUMIf(transferred_amount_gno, role = 'source') AS total_source_out_gno
    ,SUMIf(transferred_amount_gno, role = 'target') AS total_target_in_gno
    ,SUMIf(transferred_amount_gno, role = 'source') - SUMIf(transferred_amount_gno, role = 'target') AS asymmetry_gno
    ,SUMIf(cnt, role = 'source') AS source_count
    ,SUMIf(cnt, role = 'target') AS target_count
FROM {{ ref('int_consensus_validators_consolidations_daily') }}
WHERE
    {% if var('test_full_refresh', false) %}1=1
    {% else %}toDate(date) >= today() - {{ var('test_lookback_days', 30) }}
    {% endif %}
GROUP BY date
HAVING ABS(SUMIf(transferred_amount_gno, role = 'source') - SUMIf(transferred_amount_gno, role = 'target')) > 1e-6
