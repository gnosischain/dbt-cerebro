

-- q_balance is REAL GNO: source balance is gwei-of-mGNO (32 mGNO = 1 GNO),
-- converted here at the origin via /1e9/32 (this mart reads the gwei-native
-- per-index table directly). Consumers must NOT divide by 32 again.
-- q_apy is a ratio (unit-invariant).
SELECT
    date,
    q_balance[1] AS q05_balance,
    q_balance[2] AS q10_balance,
    q_balance[3] AS q25_balance,
    q_balance[4] AS q50_balance,
    q_balance[5] AS q75_balance,
    q_balance[6] AS q90_balance,
    q_balance[7] AS q95_balance,
    q_apy[1] AS q05_apy,
    q_apy[2] AS q10_apy,
    q_apy[3] AS q25_apy,
    q_apy[4] AS q50_apy,
    q_apy[5] AS q75_apy,
    q_apy[6] AS q90_apy,
    q_apy[7] AS q95_apy
FROM (
    SELECT
        (SELECT max(date) FROM  `dbt`.`int_consensus_validators_per_index_apy_daily` ) AS date
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(balance/POWER(10,9)/32) AS q_balance
        ,quantilesTDigest(
            0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95
        )(apy) AS q_apy
    FROM `dbt`.`int_consensus_validators_per_index_apy_daily` 
    WHERE date >= addDays((SELECT max(date) FROM  `dbt`.`int_consensus_validators_per_index_apy_daily` ), -30)
)