{{ config(materialized='view') }}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    e.block_number,
    e.block_timestamp,
    e.transaction_hash,
    e.log_index,
    lower(decoded_params['solver'])                                          AS solver
FROM {{ ref('contracts_CowProtocol_GPv2Settlement_events') }} e
WHERE e.event_name = 'Settlement'
  AND e.block_timestamp < today()
  AND decoded_params['solver'] IS NOT NULL
  {% if start_month and end_month %}
    AND toStartOfMonth(e.block_timestamp) >= toDate('{{ start_month }}')
    AND toStartOfMonth(e.block_timestamp) <= toDate('{{ end_month }}')
  {% endif %}
