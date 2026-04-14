



WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet
    FROM `dbt`.`int_execution_gpay_wallets`
)

SELECT
    co.safe_address       AS pay_wallet,
    co.owner              AS owner,
    co.current_threshold  AS threshold,
    co.became_owner_at    AS block_timestamp
FROM `dbt`.`int_execution_safes_current_owners` co
INNER JOIN gpay_safes gs
    ON co.safe_address = gs.pay_wallet

WHERE co.became_owner_at > (
    SELECT coalesce(max(block_timestamp), toDateTime('1970-01-01')) FROM `dbt`.`int_execution_gpay_wallet_owners`
)
