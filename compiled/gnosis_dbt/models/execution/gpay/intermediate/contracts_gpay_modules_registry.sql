

WITH gpay_safes AS (
    SELECT lower(address) AS pay_wallet
    FROM `dbt`.`int_execution_gpay_wallets`
),

-- Every (module_proxy, first_enabled_at) pair that was enabled on a GP Safe.
enabled_on_gp AS (
    SELECT
        lower(target_address) AS module_proxy,
        min(block_timestamp)  AS first_enabled_at
    FROM `dbt`.`int_execution_safes_module_events`
    WHERE event_kind = 'enabled_module'
      AND lower(safe_address) IN (SELECT pay_wallet FROM gpay_safes)
      AND target_address IS NOT NULL
    GROUP BY module_proxy
),

-- Same proxies, joined to the Zodiac factory discovery so we know
-- which mastercopy each one points at, and therefore the contract type.
typed AS (
    SELECT
        e.module_proxy                                                 AS address,
        multiIf(
            p.master_copy = '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5', 'DelayModule',
            p.master_copy = '0x9646fdad06d3e24444381f44362a3b0eb343d337', 'RolesModule',
            p.master_copy = '0x70db53617d170a4e407e00dff718099539134f9a', 'SpenderModule',
            'Unknown'
        )                                                              AS contract_type,
        p.master_copy                                                  AS abi_source_address,
        toUInt8(1)                                                     AS is_dynamic,
        e.first_enabled_at                                             AS start_blocktime,
        'gpay_module_enabled_x_proxy_factory'                          AS discovery_source
    FROM enabled_on_gp e
    INNER JOIN `dbt`.`int_execution_zodiac_module_proxies` p
        ON p.proxy_address = e.module_proxy
    WHERE p.master_copy IN (
        '0x4a97e65188a950dd4b0f21f9b5434daee0bbf9f5',
        '0x9646fdad06d3e24444381f44362a3b0eb343d337',
        '0x70db53617d170a4e407e00dff718099539134f9a'
    )
)

-- typed.address comes from int_execution_safes_module_events.target_address,
-- which is already 0x-prefixed (decode_logs writes address-typed decoded_params
-- with the prefix). Same for typed.abi_source_address from
-- int_execution_zodiac_module_proxies.master_copy. No re-prefixing.
SELECT
    address,
    contract_type,
    abi_source_address,
    is_dynamic,
    start_blocktime,
    discovery_source
FROM typed