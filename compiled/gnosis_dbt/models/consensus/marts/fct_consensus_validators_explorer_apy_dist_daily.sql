




-- Materialised passthrough of int_consensus_validators_explorer_apy_dist_daily so the
-- dashboard's `WHERE withdrawal_credentials = 'x'` filter prunes at read time via the
-- physical primary index. Same pattern as fct_consensus_validators_explorer_daily.

SELECT * FROM `dbt`.`int_consensus_validators_explorer_apy_dist_daily`
