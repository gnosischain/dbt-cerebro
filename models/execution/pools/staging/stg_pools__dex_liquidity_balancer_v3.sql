{{ config(materialized='ephemeral') }}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'Balancer V3'                                                                    AS protocol,
    concat('0x', replaceAll(lower(decoded_params['pool']), '0x', ''))               AS pool_address,
    lower(decoded_params['liquidityProvider'])                                       AS provider,
    multiIf(e.event_name = 'LiquidityAdded', 'mint', 'burn')                        AS event_type,
    coalesce(wm.underlying_address, pt.token_address)                                 AS token_address,
    abs(toInt256OrNull(replaceAll(amount_val, '"', '')))                             AS amount_raw
FROM {{ ref('contracts_BalancerV3_Vault_events') }} e
ARRAY JOIN
    range(length(JSONExtractArrayRaw(ifNull(
        multiIf(
            e.event_name = 'LiquidityAdded',   decoded_params['amountsAddedRaw'],
            e.event_name = 'LiquidityRemoved', decoded_params['amountsRemovedRaw'],
            '[]'
        ), '[]'
    )))) AS token_idx,
    JSONExtractArrayRaw(ifNull(
        multiIf(
            e.event_name = 'LiquidityAdded',   decoded_params['amountsAddedRaw'],
            e.event_name = 'LiquidityRemoved', decoded_params['amountsRemovedRaw'],
            '[]'
        ), '[]'
    )) AS amount_val
LEFT JOIN {{ ref('stg_pools__balancer_v3_pool_tokens') }} pt
    ON  pt.pool_address = replaceAll(lower(decoded_params['pool']), '0x', '')
    AND pt.token_index  = token_idx
LEFT JOIN {{ ref('stg_pools__balancer_v3_token_map') }} wm
    ON wm.wrapper_address = pt.token_address
WHERE e.event_name IN ('LiquidityAdded', 'LiquidityRemoved')
  AND e.block_timestamp < today()
  AND decoded_params['liquidityProvider'] IS NOT NULL
  {% if start_month and end_month %}
    AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
  {% endif %}
