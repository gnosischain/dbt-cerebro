{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(date, event_name)',
    unique_key='(date, event_name)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','hub_events','daily']
  )
}}

-- Daily count of every Circles v2 Hub event, broken down by event_name.
-- One row per (date, event_name) with:
--   n_events             - row count of that event on that day
--   n_tx                 - distinct transactions emitting the event
--   n_distinct_addresses - distinct addresses across all event-specific
--                          participant fields (avatar/inviter/group/org/
--                          truster/trustee/operator/from/to/human/account/
--                          backer/holder). The 0x00..00 sentinel and the
--                          empty string are excluded.
--
-- Drives the event-mix heatmap on the Circles dashboard.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    toDate(e.block_timestamp)              AS date,
    e.event_name                           AS event_name,
    countDistinct(e.transaction_hash, e.log_index) AS n_events,
    uniqExact(e.transaction_hash)          AS n_tx,
    uniqExactIf(participant, participant != '') AS n_distinct_addresses
FROM {{ ref('contracts_circles_v2_Hub_events') }} e
LEFT ARRAY JOIN
    arrayFilter(
        x -> x != '' AND x != '0x0000000000000000000000000000000000000000',
        arrayMap(addr -> lower(addr), [
            coalesce(e.decoded_params['avatar'],       ''),
            coalesce(e.decoded_params['inviter'],      ''),
            coalesce(e.decoded_params['group'],        ''),
            coalesce(e.decoded_params['organization'], ''),
            coalesce(e.decoded_params['truster'],      ''),
            coalesce(e.decoded_params['trustee'],      ''),
            coalesce(e.decoded_params['operator'],     ''),
            coalesce(e.decoded_params['from'],         ''),
            coalesce(e.decoded_params['to'],           ''),
            coalesce(e.decoded_params['human'],        ''),
            coalesce(e.decoded_params['account'],      ''),
            coalesce(e.decoded_params['backer'],       ''),
            coalesce(e.decoded_params['holder'],       '')
        ])
    ) AS participant
WHERE e.block_timestamp < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(e.block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(e.block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(
          source_field='e.block_timestamp',
          destination_field='date',
          add_and=True) }}
  {% endif %}
GROUP BY date, event_name
