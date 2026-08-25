{{
  config(
    materialized='view',
    tags=['production','staging','governance']
  )
}}

SELECT
    space_id,
    name,
    network,
    symbol,
    proposals_count,
    followers_count,
    votes_count,
    -- Space-level (current) strategy set. NOTE: each proposal snapshots its own
    -- strategy set at creation, which can differ in length/order — align a
    -- vote's vp_by_strategy to the PROPOSAL's strategies, not this one.
    arrayMap(x -> JSONExtractString(x, 'name'),    JSONExtractArrayRaw(raw_json, 'strategies')) AS strategy_names,
    arrayMap(x -> JSONExtractString(x, 'network'), JSONExtractArrayRaw(raw_json, 'strategies')) AS strategy_networks,
    ingested_at
FROM {{ source('governance', 'snapshot_space') }} FINAL
