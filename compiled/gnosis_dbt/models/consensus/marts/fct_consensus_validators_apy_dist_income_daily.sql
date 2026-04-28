

-- Materialized as a physical table so the dashboard API returns in milliseconds. Source
-- int_consensus_validators_apy_dist_income_daily is already ~1500 rows so the wrap is
-- trivial; we keep it as a separate model for consistency with the existing
-- fct_consensus_validators_apy_mean_daily / fct_consensus_validators_income_total_daily
-- pattern (materialised table fed by an incremental intermediate).
--
-- Parallel to the frozen fct_consensus_validators_dists_last_30_days lineage — both are
-- valid; the frozen one reads per_index_apy_daily (balance-focused), this one reads
-- income_daily (the spec-bounded APY source used by every new chart on the Consensus tab).

SELECT * FROM `dbt`.`int_consensus_validators_apy_dist_income_daily`