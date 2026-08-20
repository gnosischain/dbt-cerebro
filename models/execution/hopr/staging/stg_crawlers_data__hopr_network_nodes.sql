{{
    config(
        materialized='view',
        tags=['production','staging','crawlers_data','hopr']
    )
}}

/*
  Daily per-node quality snapshot from the network.hoprnet.org prober.

  DUFOUR ONLY. The prober was never ported to jura/v4, so there is no liveness or
  latency signal for the network GnosisVPN actually runs on. Do not present an
  absence of rows for jura as jura nodes being down -- they are unmeasured. Anything
  joining this must distinguish "probed and unreachable" from "never probed".

  node_address is the on-chain address in the same lowercase 0x form as the decoded
  HoprChannels source_node/destination_node, so it joins directly with no
  normalisation.

  latency_ms IS NULL IS A SIGNAL, NOT A GAP: it means the prober could not reach the
  node. That is the reachability test, so is_reachable is derived from it rather than
  from availability, which stays populated for nodes that are currently down.

  FINAL is required: ReplacingMergeTree(ingested_at), re-running a day re-inserts it.
*/

SELECT
    multiIf(network_id = 3, 'dufour', concat('network_id_', toString(network_id))) AS network,
    snapshot_date,
    node_address,
    latency_ms,
    latency_ms IS NOT NULL       AS is_reachable,
    availability_24h,
    availability_7d,
    availability_30d,
    availability_6m,
    -- Kept for shape only: the API returns NULL for every node, so a consumer that
    -- treats it as a real measure silently loses its whole population.
    availability_1y,
    first_seen,
    last_seen,
    prober_last_run
FROM {{ source('crawlers_data_hopr', 'hopr_network_nodes') }} FINAL
