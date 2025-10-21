{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    engine = 'ReplacingMergeTree()',
    order_by = '(date, bridge, source_chain, dest_chain, token)',
    unique_key = '(date, bridge, source_chain, dest_chain, token)',
    partition_by = 'toStartOfMonth(date)',
    settings = {'allow_nullable_key': 1},
    tags = ['production', 'intermediate', 'bridges']
) }}

WITH base AS (
    SELECT
        toDate(timestamp) AS date, 
        bridge,
        source_chain,
        dest_chain,
        token,
        sum(amount_token) AS volume_token,
        sum(amount_usd) AS volume_usd,
        sum(net_usd) AS net_usd,
        count() AS txs 
    FROM {{ ref('stg_crawlers_data__dune_bridge_flows') }}
    WHERE timestamp < today()
    {{ apply_monthly_incremental_filter('timestamp', 'date', 'true') }} 
    GROUP BY date, bridge, source_chain, dest_chain, token
)

SELECT * FROM base