

-- Registry of every Balancer V2 pool, used by contracts_BalancerV2_Pool_events
-- to decode pool-level events (SwapFeePercentageChanged,
-- ProtocolFeePercentageCacheUpdated).
--
-- Why a dedicated registry instead of the shared contracts_whitelist seed:
-- every V2 pool type (Weighted, ComposableStable, MetaStable, ...) inherits
-- these two events from BasePool, so they share ONE event signature. Rather
-- than fetch each pool's full ABI, we point every pool at a single reference
-- pool's ABI via abi_source_address (the proxy/abi_source pattern decode_logs
-- already supports for Circles). The reference pool
-- 0xdd439304a77f54b1f7854751ac1169b279591ef7 (a ComposableStablePool) has both
-- events registered once in the event_signatures seed; this registry maps all
-- pools to it. Keeping it separate avoids adding an abi_source_address column to
-- contracts_whitelist (which would change decode behaviour for UniswapV3/Swapr).
SELECT DISTINCT
    concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', ''))  AS address,
    'BalancerV2Pool'                                                          AS contract_type,
    '0xdd439304a77f54b1f7854751ac1169b279591ef7'                              AS abi_source_address
FROM `dbt`.`contracts_BalancerV2_Vault_events`
WHERE event_name = 'PoolRegistered'
  AND decoded_params['poolAddress'] IS NOT NULL
  AND decoded_params['poolAddress'] != ''