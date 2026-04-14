{{ config(
    materialized='view',
    tags=['production', 'execution', 'cow', 'trades', 'staging']
    ) 
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    'CoW Protocol'                                                           AS protocol,
    concat('0x', e.contract_address)                                         AS pool_address,
    lower(decoded_params['buyToken'])                                        AS token_bought_address,
    toUInt256OrNull(decoded_params['buyAmount'])                             AS amount_bought_raw,
    lower(decoded_params['sellToken'])                                       AS token_sold_address,
    toUInt256OrNull(decoded_params['sellAmount'])                            AS amount_sold_raw,
    toUInt256OrNull(decoded_params['feeAmount'])                             AS fee_amount_raw,
    lower(decoded_params['owner'])                                           AS taker,
    decoded_params['orderUid']                                               AS order_uid
FROM {{ ref('contracts_CowProtocol_GPv2Settlement_events') }} e
WHERE e.event_name = 'Trade'
  AND e.block_timestamp < today()
  AND decoded_params['buyAmount']  IS NOT NULL
  AND decoded_params['sellAmount'] IS NOT NULL
  {% if start_month and end_month %}
    AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
  {% endif %}
