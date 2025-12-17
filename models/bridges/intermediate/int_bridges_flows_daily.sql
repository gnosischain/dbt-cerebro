{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    engine = 'ReplacingMergeTree()',
    order_by = '(date, bridge, source_chain, dest_chain, token, direction)',
    unique_key = '(date, bridge, source_chain, dest_chain, token, direction)',
    partition_by = 'toStartOfMonth(date)',
    settings = {'allow_nullable_key': 1},
    tags = ['production', 'intermediate', 'bridges']
) }}

SELECT
    date,
    bridge,
    source_chain,
    dest_chain,
    token,
    direction,
    volume_token,
    volume_usd,
    net_usd,
    txs
FROM {{ ref('stg_crawlers_data__dune_bridge_flows') }}
WHERE date < today()
{{ apply_monthly_incremental_filter('date', 'date', 'true') }}