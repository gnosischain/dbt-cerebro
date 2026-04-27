{{ config(
    materialized='view',
    tags=['dev', 'execution', 'cow', 'interactions', 'staging']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    lower(decoded_params['target'])                                          AS target,
    decoded_params['value']                                                  AS value,
    decoded_params['selector']                                               AS selector
FROM {{ ref('contracts_CowProtocol_GPv2Settlement_events') }} e
WHERE e.event_name = 'Interaction'
  AND e.block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
  {% endif %}
