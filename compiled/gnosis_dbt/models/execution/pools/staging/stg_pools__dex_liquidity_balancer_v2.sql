WITH balancer_v2_pool_registry AS (
    SELECT DISTINCT
        lower(decoded_params['poolId'])                                          AS pool_id,
        concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
    FROM `dbt`.`contracts_BalancerV2_Vault_events`
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['poolId']      IS NOT NULL
      AND decoded_params['poolAddress'] IS NOT NULL
)

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Balancer V2'                                   AS protocol,
    r.pool_address                                  AS pool_address,
    lower(decoded_params['liquidityProvider'])      AS provider,
    multiIf(
        multiIf(
            startsWith(replaceAll(delta_val, '"', ''), '-'),
            toInt256OrNull(replaceAll(delta_val, '"', '')),
            reinterpretAsInt256(toUInt256OrNull(replaceAll(delta_val, '"', '')))
        ) >= 0,
        'mint', 'burn'
    )                                               AS event_type,
    lower(replaceAll(token_val, '"', ''))           AS token_address,
    abs(multiIf(
        startsWith(replaceAll(delta_val, '"', ''), '-'),
        toInt256OrNull(replaceAll(delta_val, '"', '')),
        reinterpretAsInt256(toUInt256OrNull(replaceAll(delta_val, '"', '')))
    ))                                              AS amount_raw,
    CAST(NULL AS Nullable(Int32))                   AS tick_lower,
    CAST(NULL AS Nullable(Int32))                   AS tick_upper,
    CAST(NULL AS Nullable(Int256))                  AS liquidity_delta
FROM `dbt`.`contracts_BalancerV2_Vault_events` e
LEFT JOIN balancer_v2_pool_registry r
    ON r.pool_id = lower(decoded_params['poolId'])
ARRAY JOIN
    JSONExtractArrayRaw(ifNull(decoded_params['tokens'], '[]')) AS token_val,
    JSONExtractArrayRaw(ifNull(decoded_params['deltas'],  '[]')) AS delta_val
WHERE e.event_name = 'PoolBalanceChanged'
  AND e.block_timestamp < today()
  AND decoded_params['liquidityProvider'] IS NOT NULL
  AND decoded_params['tokens']            IS NOT NULL
  AND decoded_params['deltas']            IS NOT NULL
  