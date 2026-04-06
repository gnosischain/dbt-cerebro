{{ config(materialized='ephemeral') }}

{%- set start_month = var('start_month', none) -%}
{%- set end_month   = var('end_month',   none) -%}

WITH balancer_v2_pool_registry AS (
    SELECT DISTINCT
        lower(decoded_params['poolId'])                                          AS pool_id,
        concat('0x', replaceAll(lower(decoded_params['poolAddress']), '0x', '')) AS pool_address
    FROM {{ ref('contracts_BalancerV2_Vault_events') }}
    WHERE event_name = 'PoolRegistered'
      AND decoded_params['poolId']      IS NOT NULL
      AND decoded_params['poolAddress'] IS NOT NULL
)

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Balancer V2'                                                            AS protocol,
    r.pool_address                                                           AS pool_address,
    lower(decoded_params['tokenOut'])                                        AS token_bought_address,
    toUInt256OrNull(decoded_params['amountOut'])                             AS amount_bought_raw,
    lower(decoded_params['tokenIn'])                                         AS token_sold_address,
    toUInt256OrNull(decoded_params['amountIn'])                              AS amount_sold_raw,
    CAST(NULL AS Nullable(String))                                           AS taker
FROM {{ ref('contracts_BalancerV2_Vault_events') }} e
LEFT JOIN balancer_v2_pool_registry r
    ON r.pool_id = lower(decoded_params['poolId'])
WHERE e.event_name = 'Swap'
  AND e.block_timestamp < today()
  AND decoded_params['tokenIn']   IS NOT NULL
  AND decoded_params['tokenOut']  IS NOT NULL
  AND decoded_params['amountIn']  IS NOT NULL
  AND decoded_params['amountOut'] IS NOT NULL
  {% if start_month and end_month %}
    AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
  {% endif %}
