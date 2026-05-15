{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, mint_kind)',
    unique_key='(date, mint_kind)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','mints','daily']
  )
}}

-- Network-level daily mint summary, broken down by mint_kind.
--   mint_kind      - 'personal' | 'group' | 'migration' | 'other'
--   n_mint_events  - number of mint TransferSingle events
--   n_minters      - distinct recipient addresses minting that day
--   amount_minted  - total CRC minted (raw / 1e18)
--
-- Source: int_execution_circles_v2_mint_events (which classifies each
-- mint via the avatar registry + V2 Hub call-decoder; see that model
-- for the classifier details). Replaces the previous logic that filtered
-- int_execution_circles_v2_hub_transfers on from-zero alone — that
-- predicate lumped personal mints, group mints, and V1→V2 migrations
-- together.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(block_timestamp)                          AS date,
    mint_kind                                        AS mint_kind,
    count()                                          AS n_mint_events,
    uniqExact(to_address)                            AS n_minters,
    sum(toFloat64(amount_raw)) / pow(10, 18)         AS amount_minted
FROM {{ ref('int_execution_circles_v2_mint_events') }}
WHERE block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='block_timestamp',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date, mint_kind
