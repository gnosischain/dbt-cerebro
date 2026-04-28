

-- Balance distribution histogram across co-validators sharing a credential.
-- Light view over fct_consensus_validators_status_latest (558k rows); the API
-- prunes by withdrawal_credentials before the bucketing runs so this is cheap
-- even without a physical index on credentials.
--
-- Bucket edges chosen to span the two MaxEB regimes:
--   * 0x00 / 0x01 credentials cap at 32 GNO effective balance (pre-Pectra).
--   * 0x02 compounders + EIP-7251 go up to 2048 GNO post-Pectra.
-- The 16 / 32 / 64 / 128 / 256 edges surface clusters at both caps plus any
-- partial-balance validators (withdrawing or slashed).

WITH per_validator AS (
    SELECT
        withdrawal_credentials
        ,effective_balance / 1e9 AS balance_gno
    FROM `dbt`.`fct_consensus_validators_status_latest`
)

SELECT
    withdrawal_credentials
    ,CASE
        WHEN balance_gno < 1   THEN '<1'
        WHEN balance_gno < 16  THEN '1-16'
        WHEN balance_gno < 32  THEN '16-32'
        WHEN balance_gno < 48  THEN '32-48'
        WHEN balance_gno < 64  THEN '48-64'
        WHEN balance_gno < 128 THEN '64-128'
        WHEN balance_gno < 256 THEN '128-256'
        ELSE '>=256'
     END AS bucket
    -- Preserve bucket sort order for the API `sort` above.
    ,CASE
        WHEN balance_gno < 1   THEN 0
        WHEN balance_gno < 16  THEN 1
        WHEN balance_gno < 32  THEN 2
        WHEN balance_gno < 48  THEN 3
        WHEN balance_gno < 64  THEN 4
        WHEN balance_gno < 128 THEN 5
        WHEN balance_gno < 256 THEN 6
        ELSE 7
     END AS bucket_order
    ,COUNT(*) AS validator_count
    ,SUM(balance_gno) AS balance_gno_total
FROM per_validator
GROUP BY withdrawal_credentials, bucket, bucket_order
ORDER BY withdrawal_credentials, bucket_order