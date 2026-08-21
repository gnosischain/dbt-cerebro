

/*
  Daily HOPR network health: how many nodes have ever registered, versus how many are
  actually up.

  THE GAP BETWEEN THOSE TWO IS THE POINT OF THIS MODEL. On-chain registration is
  cumulative and never expires -- a node that ran once in 2023 and vanished is still
  counted in nodes_registered_cumulative forever. The prober and the dashboard's
  online series measure the live network. The two differ by roughly 4x, so quoting
  the registry as "network size" overstates HOPR by that factor. Both columns are
  here, side by side, specifically so a chart cannot show one while implying the
  other.

  COVERAGE IS ASYMMETRIC AND MOSTLY dufour:
    - nodes_online_* comes from the dashboard history and reaches back to 2023-09.
    - nodes_probed/reachable/availability start only when click-runner's hopr-network
      ingestor first ran, so they are NULL for the whole earlier span. NULL means
      unmeasured, never zero.
    - both prober feeds are dufour-only (never ported to v4), so jura has registration
      counts and nothing else. Do not read jura's missing liveness as jura being down.

  hours_observed IS NOT DECORATIVE. The dashboard's hourly history is missing roughly
  a fifth of its hours, so a day can be backed by anything from 1 to 24 observations.
  Always read nodes_online_avg together with hours_observed, and drop or flag thin
  days rather than plotting them as equal to full ones.

  The date spine is the union of dates actually observed in the three inputs -- no
  date is invented, so a missing day is visibly missing rather than interpolated.
*/

WITH

-- A node's registration date is its first KeyBinding: the act of binding a packet key
-- to a chain key is how a node joins the network. AddressAnnouncement can repeat as a
-- node re-announces a new address, so it dates announcements, not registrations.
node_first_seen AS (
    SELECT
        r.network                                   AS network,
        lower(a.decoded_params['chain_key'])        AS node_address,
        min(toDate(a.block_timestamp))              AS first_date
    FROM `dbt`.`contracts_hopr_Announcements_events` AS a
    INNER JOIN `dbt`.`contracts_hopr_registry` AS r
        -- execution.logs carries addresses as BARE hex while the registry stores them
        -- 0x-prefixed. Joining them raw matches nothing, produces a cumulative count of
        -- zero, and still builds green -- so strip the prefix, as int_hopr_channels_events
        -- does. The `registrations are non-zero` validation guards the regression.
        ON lower(a.contract_address) = replaceAll(r.address, '0x', '')
    WHERE a.event_name = 'KeyBinding'
      AND a.decoded_params['chain_key'] != ''
      -- Production networks only -- rotsee is the v4 testnet. Its registrations are
      -- test deployments and would inflate the cumulative node count, which is the
      -- headline series of this mart.
      AND NOT r.is_testnet
    GROUP BY network, node_address
),

registrations AS (
    SELECT network, first_date AS date, count() AS nodes_registered_new
    FROM node_first_seen
    GROUP BY network, date
),

online AS (
    SELECT
        network,
        toDate(observed_at)         AS date,
        round(avg(nodes_online))    AS nodes_online_avg,
        min(nodes_online)           AS nodes_online_min,
        max(nodes_online)           AS nodes_online_max,
        uniqExact(observed_at)      AS hours_observed
    FROM `dbt`.`stg_hopr_db__network_online_hourly`
    GROUP BY network, date
),

probed AS (
    SELECT
        network,
        snapshot_date                                       AS date,
        count()                                             AS nodes_probed,
        countIf(is_reachable)                               AS nodes_reachable,
        -- "High availability" is the operator-quality tier: reachable AND up at least
        -- 90% of the last day. Kept separate from nodes_reachable because a node can
        -- answer a single probe while being down most of the time.
        countIf(availability_24h >= 0.9)                    AS nodes_high_availability,
        round(avgIf(latency_ms, is_reachable), 1)           AS avg_latency_ms,
        round(quantileIf(0.5)(latency_ms, is_reachable), 1) AS p50_latency_ms,
        round(avg(availability_24h), 4)                     AS avg_availability_24h
    FROM `dbt`.`stg_hopr_db__network_nodes`
    GROUP BY network, date
),

spine AS (
    SELECT network, date FROM registrations
    UNION DISTINCT
    SELECT network, date FROM online
    UNION DISTINCT
    SELECT network, date FROM probed
)

SELECT
    s.network                               AS network,
    s.date                                  AS date,

    coalesce(g.nodes_registered_new, 0)     AS nodes_registered_new,
    -- Cumulative over the spine, which contains every date that has any activity, so
    -- the running total is correct at every row it emits.
    sum(coalesce(g.nodes_registered_new, 0)) OVER (
        PARTITION BY s.network ORDER BY s.date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                       AS nodes_registered_cumulative,

    o.nodes_online_avg                      AS nodes_online_avg,
    o.nodes_online_min                      AS nodes_online_min,
    o.nodes_online_max                      AS nodes_online_max,
    coalesce(o.hours_observed, 0)           AS hours_observed,

    p.nodes_probed                          AS nodes_probed,
    p.nodes_reachable                       AS nodes_reachable,
    p.nodes_high_availability               AS nodes_high_availability,
    p.avg_latency_ms                        AS avg_latency_ms,
    p.p50_latency_ms                        AS p50_latency_ms,
    p.avg_availability_24h                  AS avg_availability_24h

FROM spine AS s
LEFT JOIN registrations AS g ON s.network = g.network AND s.date = g.date
LEFT JOIN online        AS o ON s.network = o.network AND s.date = o.date
LEFT JOIN probed        AS p ON s.network = p.network AND s.date = p.date