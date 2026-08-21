{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(network, node_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','hopr','intermediate'],
    pre_hook=["SET allow_experimental_json_type = 1", "SET join_use_nulls = 1"],
    post_hook=["SET allow_experimental_json_type = 0", "SET join_use_nulls = 0"]
  )
}}

/*
  join_use_nulls = 1 IS REQUIRED, NOT COSMETIC -- read this before removing it.

  Every enrichment below is a LEFT JOIN, and ClickHouse's default fills unmatched
  rows with the column TYPE DEFAULT ('' for String) rather than NULL. Without the
  setting, coalesce(reg.node_class, 'unclassified') returns '' so every unclassified
  node reads as classified, and `ip.ip IS NOT NULL` is true for unmatched rows so
  every node reports geo enrichment whether or not ipinfo matched. Both failures are
  completely silent.
  Hooks come in pairs, so the post_hook restores the default.
*/

/*
  HOPR node dimension -- one row per (network, node_address).

  This is the model that turns three previously dead-end decode streams into
  something usable. Before it, only contracts_hopr_Channels_events had any
  consumer; Announcements, NodeSafeRegistry and NodeStakeFactory were decoded but
  referenced by nothing.

  What it joins, and why the node is the right grain:

    node chain key -> Safe            (NodeSafeRegistry)   -> operator identity
                   -> announced IP    (Announcements)      -> geo / ASN / hosting
                   -> class           (hopr_node_registry)  -> cover traffic vs VPN exit
                   -> earnings        (int_hopr_channels_events, as destination)

  The operator dimension is the point. `safe_address` is what makes
  nodes-per-operator computable, which is the one HOPR decentralisation metric
  that is not a vanity count -- on jura, 15+ of the first 16 nodes sit under a
  single HOPR-operated Safe, and no node count conveys that.

  THREE THINGS THAT WILL BITE A FUTURE EDITOR:

  1. dufour MISSPELLS the deregistration event as `DergisteredNodeSafe`; jura
     spells it `DeregisteredNodeSafe`. Filtering on one spelling silently
     under-counts deregistrations on the other network. Both are matched here.

  2. `NewHoprNodeStakeSafe` carries only `instance` (the Safe address) -- it does
     NOT carry a node address, so it cannot link a node to a Safe. Only
     NodeSafeRegistry does that. The factory event is used solely to date the
     Safe's creation.

  3. Geo coverage is PARTIAL and that is visible, not hidden. crawlers_data.ipinfo
     was populated by the nebula/ip_crawler pipeline for a different node set, so it
     contains HOPR IPs only incidentally until ip_crawler's `hopr` source preset has
     been run against the database being read. `geo_source` records which bucket each
     node fell into so an unenriched node is never silently read as "no country".
     Do not filter unenriched nodes out of counts.

  4. Liveness IS joined (latency_ms, availability_*, prober_snapshot_date) from the
     network.hoprnet.org prober via stg_hopr_db__network_nodes. It is
     dufour-only -- the prober was never ported to jura/v4 -- and `liveness_source`
     separates "probed and unreachable" from "never probed" from "no prober covers
     this network". Absence from the prober's roster does NOT prove a node is dead:
     some unprobed nodes still earn on chain, so judge liveness with
     last_channel_activity_at as well.
*/

WITH announcements AS (
    SELECT
        r.network                                       AS network,
        lower(e.decoded_params['node'])                 AS node_address,
        e.decoded_params['baseMultiaddr']               AS multiaddr,
        e.block_timestamp                               AS block_timestamp,
        e.block_number                                  AS block_number
    FROM {{ ref('contracts_hopr_Announcements_events') }} AS e
    INNER JOIN {{ ref('contracts_hopr_registry') }}       AS r
        ON lower(e.contract_address) = replaceAll(r.address, '0x', '')
    WHERE e.event_name = 'AddressAnnouncement'
      AND NOT empty(coalesce(e.decoded_params['node'], ''))
),

-- Latest announcement per node: a node re-announces when its address changes, so
-- only the most recent multiaddr describes where it is now.
latest_announcement AS (
    SELECT
        network,
        node_address,
        argMax(multiaddr, (block_number, block_timestamp))  AS multiaddr,
        max(block_timestamp)                                AS last_announced_at,
        min(block_timestamp)                                AS first_announced_at,
        count()                                             AS announcement_count
    FROM announcements
    GROUP BY network, node_address
),

key_bindings AS (
    SELECT
        r.network                                       AS network,
        lower(e.decoded_params['chain_key'])            AS node_address,
        argMax(e.decoded_params['ed25519_pub_key'], e.block_number) AS packet_pub_key,
        min(e.block_timestamp)                          AS first_key_binding_at
    FROM {{ ref('contracts_hopr_Announcements_events') }} AS e
    INNER JOIN {{ ref('contracts_hopr_registry') }}       AS r
        ON lower(e.contract_address) = replaceAll(r.address, '0x', '')
    WHERE e.event_name = 'KeyBinding'
      AND NOT empty(coalesce(e.decoded_params['chain_key'], ''))
    GROUP BY network, node_address
),

-- NodeSafeRegistry is the ONLY source that links a node to its Safe.
-- Match both spellings of the deregistration event (see header note 1).
safe_links AS (
    SELECT
        r.network                                       AS network,
        lower(e.decoded_params['nodeAddress'])          AS node_address,
        lower(e.decoded_params['safeAddress'])          AS safe_address,
        e.event_name                                    AS event_name,
        e.block_timestamp                               AS block_timestamp,
        e.block_number                                  AS block_number
    FROM {{ ref('contracts_hopr_NodeSafeRegistry_events') }} AS e
    INNER JOIN {{ ref('contracts_hopr_registry') }}          AS r
        ON lower(e.contract_address) = replaceAll(r.address, '0x', '')
    WHERE e.event_name IN ('RegisteredNodeSafe', 'DergisteredNodeSafe', 'DeregisteredNodeSafe')
      AND NOT empty(coalesce(e.decoded_params['nodeAddress'], ''))
),

safe_state AS (
    SELECT
        network,
        node_address,
        argMax(safe_address, (block_number, block_timestamp))  AS safe_address,
        argMax(event_name, (block_number, block_timestamp))    AS last_registry_event,
        minIf(block_timestamp, event_name = 'RegisteredNodeSafe')  AS first_registered_at,
        maxIf(block_timestamp, event_name != 'RegisteredNodeSafe') AS last_deregistered_at
    FROM safe_links
    GROUP BY network, node_address
),

-- Safe creation date, keyed on the Safe rather than the node: the factory event
-- carries only `instance` (see header note 2).
safe_created AS (
    SELECT
        r.network                                       AS network,
        lower(e.decoded_params['instance'])             AS safe_address,
        min(e.block_timestamp)                          AS safe_created_at
    FROM {{ ref('contracts_hopr_NodeStakeFactory_events') }} AS e
    INNER JOIN {{ ref('contracts_hopr_registry') }}          AS r
        ON lower(e.contract_address) = replaceAll(r.address, '0x', '')
    WHERE e.event_name = 'NewHoprNodeStakeSafe'
      AND NOT empty(coalesce(e.decoded_params['instance'], ''))
    GROUP BY network, safe_address
),

-- Channel-side activity. `earned_*` is what this node was paid AS A DESTINATION
-- (relayer); `channels_opened` is what it initiated AS A SOURCE (payer).
channel_activity AS (
    SELECT
        network,
        node_address,
        sumIf(redeemed_wei, is_destination = 1)                       AS earned_wei,
        countIf(is_destination = 1 AND event_name = 'TicketRedeemed')  AS tickets_earned,
        uniqExactIf(channel_id, is_destination = 1)                    AS channels_as_destination,
        uniqExactIf(channel_id, is_destination = 0)                    AS channels_as_source,
        countIf(is_destination = 0 AND event_name = 'ChannelOpened')    AS channels_opened,
        min(block_timestamp)                                           AS first_channel_activity_at,
        max(block_timestamp)                                           AS last_channel_activity_at
    FROM (
        SELECT network, destination_node AS node_address, 1 AS is_destination,
               redeemed_wei, event_name, channel_id, block_timestamp
        FROM {{ ref('int_hopr_channels_events') }}
        WHERE destination_node IS NOT NULL
        UNION ALL
        SELECT network, source_node AS node_address, 0 AS is_destination,
               toNullable(toUInt256(0)) AS redeemed_wei, event_name, channel_id, block_timestamp
        FROM {{ ref('int_hopr_channels_events') }}
        WHERE source_node IS NOT NULL
    )
    GROUP BY network, node_address
),

-- Spine: every node observed anywhere. Announced-only, registry-only and
-- channel-only nodes all exist in practice, so no single source is a complete
-- population.
spine AS (
    SELECT network, node_address FROM latest_announcement
    UNION DISTINCT
    SELECT network, node_address FROM safe_state
    UNION DISTINCT
    SELECT network, node_address FROM channel_activity
),

-- crawlers_data.ipinfo is a plain MergeTree ORDER BY (ip, updated_at), NOT a
-- ReplacingMergeTree, so it legitimately holds more than one row per IP:
-- ip_crawler re-fetches an IP whenever it retries, and each attempt appends.
-- Joining it raw would FAN OUT and silently duplicate a node row, inflating every
-- operator-concentration and geography aggregate. None of the currently matched
-- HOPR IPs happen to be duplicated, which is exactly why the grain test passes
-- today and would keep passing right up until it broke. Collapse to one row per
-- IP, latest wins.
ipinfo_latest AS (
    SELECT
        ip                                              AS ip,
        argMax(country, updated_at)                     AS country,
        argMax(city, updated_at)                        AS city,
        argMax(org, updated_at)                         AS org,
        argMax(asn, updated_at)                         AS asn,
        argMax(generic_provider, updated_at)            AS generic_provider,
        argMax(is_mobile, updated_at)                   AS is_mobile,
        argMax(loc, updated_at)                         AS loc
    FROM {{ ref('stg_crawlers_data__ipinfo') }}
    GROUP BY ip
),

-- Coordinates are parsed HERE, not in the final SELECT, and that placement is load
-- bearing. ipinfo returns them as one "lat,lon" string. After the LEFT JOIN below,
-- join_use_nulls makes loc Nullable(String), and splitByChar over a Nullable yields
-- Nullable(Array(String)) -- a type ClickHouse refuses outright (code 43, "Nested type
-- Array(String) cannot be inside Nullable type"). Inside this CTE loc is still a plain
-- String, so the split is legal; the join then makes the two Floats nullable, which is
-- exactly what we want for a node with no ipinfo row.
ipinfo_geo AS (
    SELECT
        ip,
        country,
        city,
        org,
        asn,
        generic_provider,
        is_mobile,
        -- toFloat64OrNull, not toFloat64: a missing or malformed loc must yield NULL, not
        -- 0. 0,0 is a real place in the Gulf of Guinea and would plot every unlocated
        -- node there, which reads as a cluster rather than as missing data.
        toFloat64OrNull(splitByChar(',', loc)[1])        AS latitude,
        toFloat64OrNull(splitByChar(',', loc)[2])        AS longitude
    FROM ipinfo_latest
),

-- The prober publishes a fresh snapshot per day, so take the latest row per node.
-- Its universe is much smaller than ours: it tracks its own roster, while this model's
-- spine is every node that ever appeared on chain.
--
-- THE ROSTER IS NOT THE POPULATION OF RECORD. Absence from it correlates with
-- dormancy but does not prove it: a meaningful minority of unprobed nodes are still
-- being PAID on chain, some of them today. So absence is recorded in liveness_source
-- as its own state rather than collapsed into "down" -- treating not_probed as dead
-- would silently write off live, earning nodes.
prober_latest AS (
    SELECT
        network                                         AS network,
        node_address                                    AS node_address,
        max(snapshot_date)                              AS prober_snapshot_date,
        argMax(latency_ms, snapshot_date)               AS latency_ms,
        argMax(is_reachable, snapshot_date)             AS is_reachable,
        argMax(availability_24h, snapshot_date)         AS availability_24h,
        argMax(availability_7d, snapshot_date)          AS availability_7d,
        argMax(availability_30d, snapshot_date)         AS availability_30d
    FROM {{ ref('stg_hopr_db__network_nodes') }}
    -- network is part of the key even though the prober only covers dufour today.
    -- Grouping on node_address alone would collapse the same address across networks
    -- and let argMax hand back another network's measurement -- silently, and only
    -- once someone widens HOPR_NETWORK_IDS, which the CLI accepts as a list.
    GROUP BY network, node_address
),

-- Which networks the prober covers AT ALL, derived rather than hardcoded. This is
-- what separates "probed and down" from "this network has no prober", and deriving it
-- means the day the prober gains v4 support the classification follows automatically
-- instead of quietly reporting every jura node as unmeasurable forever.
prober_networks AS (
    SELECT DISTINCT network FROM {{ ref('stg_hopr_db__network_nodes') }}
),

-- One row per network, so joining it cannot fan out. Pulled from the registry
-- rather than re-testing the name here: the testnet rule is defined once, in
-- contracts_hopr_registry, and every consumer inherits it.
network_flags AS (
    SELECT DISTINCT network, is_testnet
    FROM {{ ref('contracts_hopr_registry') }}
),

enriched AS (
    SELECT
        s.network                                       AS network,
        nf.is_testnet                                   AS is_testnet,
        s.node_address                                  AS node_address,

        -- Operator identity
        st.safe_address                                 AS safe_address,
        toUInt8(st.last_registry_event = 'RegisteredNodeSafe') AS is_registered,
        st.first_registered_at                          AS first_registered_at,
        nullIf(st.last_deregistered_at, toDateTime(0))   AS last_deregistered_at,
        sc.safe_created_at                              AS safe_created_at,

        -- Identity / transport
        kb.packet_pub_key                               AS packet_pub_key,
        la.multiaddr                                    AS announced_multiaddr,
        la.announcement_count                           AS announcement_count,
        la.first_announced_at                           AS first_announced_at,
        la.last_announced_at                            AS last_announced_at,

        -- IPv4 out of the multiaddr. `[0-9.]+` deliberately avoids backslash
        -- escaping; /dns4/ and /ip6/ multiaddrs yield '' and are classified below.
        nullIf(extract(coalesce(la.multiaddr, ''), '/ip4/([0-9.]+)'), '') AS announced_ip,

        -- Declared classification (HOPR's own repos, never inferred)
        coalesce(reg.node_class, 'unclassified')        AS node_class,
        reg.label                                       AS node_label,
        nullIf(coalesce(reg.location_city, ''), '')     AS declared_city,
        nullIf(coalesce(reg.country_code, ''), '')      AS declared_country,

        -- Channel economics
        coalesce(ca.earned_wei, toUInt256(0))           AS earned_wei,
        coalesce(ca.tickets_earned, 0)                  AS tickets_earned,
        coalesce(ca.channels_as_destination, 0)         AS channels_as_destination,
        coalesce(ca.channels_as_source, 0)              AS channels_as_source,
        coalesce(ca.channels_opened, 0)                 AS channels_opened,
        ca.first_channel_activity_at                    AS first_channel_activity_at,
        ca.last_channel_activity_at                     AS last_channel_activity_at
    FROM spine AS s
    LEFT JOIN network_flags       AS nf ON nf.network = s.network
    LEFT JOIN latest_announcement AS la ON s.network = la.network AND s.node_address = la.node_address
    LEFT JOIN safe_state          AS st ON s.network = st.network AND s.node_address = st.node_address
    LEFT JOIN safe_created        AS sc ON s.network = sc.network AND st.safe_address = sc.safe_address
    LEFT JOIN key_bindings        AS kb ON s.network = kb.network AND s.node_address = kb.node_address
    LEFT JOIN channel_activity    AS ca ON s.network = ca.network AND s.node_address = ca.node_address
    LEFT JOIN {{ ref('hopr_node_registry') }} AS reg ON s.node_address = lower(reg.node_address)
)

SELECT
    e.network                                           AS network,
    e.is_testnet                                        AS is_testnet,
    e.node_address                                      AS node_address,
    e.safe_address                                      AS safe_address,
    e.is_registered                                     AS is_registered,
    e.first_registered_at                               AS first_registered_at,
    e.last_deregistered_at                              AS last_deregistered_at,
    e.safe_created_at                                   AS safe_created_at,
    e.packet_pub_key                                    AS packet_pub_key,
    e.announced_multiaddr                               AS announced_multiaddr,
    e.announced_ip                                      AS announced_ip,
    e.announcement_count                                AS announcement_count,
    e.first_announced_at                                AS first_announced_at,
    e.last_announced_at                                 AS last_announced_at,
    e.node_class                                        AS node_class,
    e.node_label                                        AS node_label,
    e.declared_city                                     AS declared_city,
    e.declared_country                                  AS declared_country,

    -- Geo / hosting, from the same ipinfo staging view the p2p models use.
    ip.country                                          AS ip_country,
    ip.city                                             AS ip_city,

    -- Already parsed in ipinfo_geo (see that CTE for why it cannot be done here).
    ip.latitude                                         AS ip_latitude,
    ip.longitude                                        AS ip_longitude,
    ip.org                                              AS ip_org,
    ip.asn                                              AS ip_asn,
    ip.generic_provider                                 AS hosting_provider,
    ip.is_mobile                                        AS ip_is_mobile,

    -- Coverage is explicit so partial enrichment can never read as "no country".
    multiIf(
        e.announced_ip IS NULL,      'no_ipv4',
        ip.ip IS NOT NULL,           'ipinfo',
        'unenriched'
    )                                                   AS geo_source,

    -- Liveness, from the network.hoprnet.org prober. On-chain presence is cumulative
    -- and never expires, so without these columns this model cannot distinguish a node
    -- running today from one that registered in 2023 and vanished -- a distinction
    -- worth several times the node count. See fct_hopr_network_health_daily.
    pr.latency_ms                                       AS latency_ms,
    pr.is_reachable                                     AS is_reachable,
    pr.availability_24h                                 AS availability_24h,
    pr.availability_7d                                  AS availability_7d,
    pr.availability_30d                                 AS availability_30d,
    pr.prober_snapshot_date                             AS prober_snapshot_date,

    -- Same honesty contract as geo_source above: never let "we did not measure this"
    -- read as "this node is down". jura has no prober at all (never ported to v4), so
    -- its nodes are unmeasurable rather than unreachable, and the two must not be
    -- filtered together.
    multiIf(
        pn.network IS NULL,          'no_prober_for_network',
        pr.node_address IS NOT NULL, 'prober',
        'not_probed'
    )                                                   AS liveness_source,

    e.earned_wei                                        AS earned_wei,
    toDecimal128(e.earned_wei / 1e18, 18)               AS earned_wxhopr,
    e.tickets_earned                                    AS tickets_earned,
    e.channels_as_destination                           AS channels_as_destination,
    e.channels_as_source                                AS channels_as_source,
    e.channels_opened                                   AS channels_opened,
    e.first_channel_activity_at                         AS first_channel_activity_at,
    e.last_channel_activity_at                          AS last_channel_activity_at
FROM enriched AS e
LEFT JOIN ipinfo_geo AS ip
    ON ip.ip = e.announced_ip
LEFT JOIN prober_latest AS pr
    ON pr.network = e.network AND pr.node_address = e.node_address
LEFT JOIN prober_networks AS pn
    ON pn.network = e.network
