{{
    config(
        materialized='view',
        tags=['production','staging','crawlers_data','hopr']
    )
}}

/*
  Hourly online-node count from the network.hoprnet.org dashboard's own history.

  This is the ONLY long-history HOPR series we have -- it reaches back to 2023-09,
  where every other feed starts the day its ingestor first ran. It is also the only
  measure of how many nodes are actually UP, as opposed to how many ever registered
  on-chain (int_hopr_nodes, cumulative and never expiring). The two differ by
  roughly 4x, so they are not interchangeable.

  THE SERIES HAS HOLES. Distinct observed hours run well below the hours spanned --
  the dashboard's history simply lacks them. Never assume 24 rows per day: aggregate
  with an explicit hours_observed count so a sparsely-observed day is visibly
  sparse rather than silently averaged as if complete.

  FINAL is required: ReplacingMergeTree(ingested_at), and a re-backfill re-inserts
  every hour it already holds.
*/

SELECT
    -- The dashboard identifies networks by its own env id, not by name. 3 is dufour
    -- (the `?env=3` the public API uses). The prober was never ported to v4, so jura
    -- never appears here; an unrecognised id is surfaced rather than silently dropped.
    multiIf(network_id = 3, 'dufour', concat('network_id_', toString(network_id))) AS network,
    observed_at,
    nodes_online
FROM {{ source('hopr_db', 'hopr_network_online_hourly') }} FINAL
