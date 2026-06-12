

-- Addresses that are NOT users: the exclusion set that defines the
-- per-user "user" universe (users = EOAs + Safes) for revenue attribution.
-- An address is a user unless it is a known contract, with Safe proxies
-- exempted; Gnosis Pay operational wallets are excluded even when they
-- are Safes.
--
-- Trace-derived sources (execution.contracts, execution.code_diffs) have
-- sparse indexing gaps (e.g. the aGnosDAI proxy 0x7a5c38... appears in
-- neither), so curated registries are unioned in as belt-and-braces:
-- token whitelist, lending market tokens/pools, DEX pool registry and the
-- contracts whitelist.

WITH contract_addresses AS (
    SELECT lower(assumeNotNull(contract_address)) AS address
    FROM `execution`.`contracts`
    WHERE contract_address IS NOT NULL

    UNION ALL

    SELECT lower(assumeNotNull(address)) AS address
    FROM `execution`.`code_diffs`
    WHERE address IS NOT NULL

    UNION ALL
    SELECT lower(address) AS address FROM `dbt`.`tokens_whitelist`

    UNION ALL
    SELECT lower(atoken_address) AS address FROM `dbt`.`atoken_reserve_mapping`

    UNION ALL
    SELECT lower(supply_token_address) AS address
    FROM `dbt`.`lending_market_mapping` WHERE supply_token_address != ''

    UNION ALL
    SELECT lower(stable_debt_token_address) AS address
    FROM `dbt`.`lending_market_mapping` WHERE stable_debt_token_address != ''

    UNION ALL
    SELECT lower(variable_debt_token_address) AS address
    FROM `dbt`.`lending_market_mapping` WHERE variable_debt_token_address != ''

    UNION ALL
    SELECT lower(pool_address) AS address
    FROM `dbt`.`lending_market_mapping` WHERE pool_address != ''

    UNION ALL
    SELECT lower(address) AS address FROM `dbt`.`contracts_whitelist`

    UNION ALL
    SELECT lower(pool_address) AS address FROM `dbt`.`stg_pools__v3_pool_registry`
)

SELECT DISTINCT assumeNotNull(address) AS address
FROM (
    SELECT address
    FROM contract_addresses
    WHERE address IS NOT NULL
      AND address NOT IN (SELECT lower(address) FROM `dbt`.`contracts_safe_registry`)

    UNION ALL

    SELECT lower(address) AS address FROM `dbt`.`gpay_operational_wallets`
)