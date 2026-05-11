{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='date',
    unique_key='(date)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','mints','daily']
  )
}}

-- Network-level daily personal-mint summary.
--   n_mint_events  - number of mint TransferSingle events
--   n_minters      - distinct avatars minting that day
--   amount_minted  - total CRC minted (raw / 1e18)
--
-- Source: int_execution_circles_v2_hub_transfers filtered to mint events
-- (from_address = 0x00…00, to_address = recipient avatar). Mirrors the
-- semantics of api_execution_circles_v2_avatar_mint_activity_daily but
-- collapsed to a single row per day.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                          AS date,
    count()                                          AS n_mint_events,
    uniqExact(to_address)                            AS n_minters,
    sum(toFloat64(amount_raw)) / pow(10, 18)         AS amount_minted
FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
WHERE from_address = '0x0000000000000000000000000000000000000000'
  AND to_address  != '0x0000000000000000000000000000000000000000'
  AND block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='block_timestamp',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date
