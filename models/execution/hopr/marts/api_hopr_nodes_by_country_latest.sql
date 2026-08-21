{{
    config(
        materialized='view',
        tags=['production','hopr','tier1','api:hopr_nodes_by_country','granularity:latest']
    )
}}

/*
  Current node counts per network per country.

  UNRESOLVED NODES ARE A ROW, NOT A FILTER. Nodes with no resolvable IPv4 (they announced
  a dns4/ip6 address or never announced) and nodes whose IP ipinfo has not been asked
  about appear under country_code 'UNKNOWN' with geo_resolved = 0. Dropping them would
  report a smaller, tidier network than exists and would quietly change the denominator of
  every share a chart computes. Coverage depends on ip_crawler having run against the
  database being read, so the UNKNOWN row grows between crawler runs.

  live_nodes counts only nodes the prober reached. It is NOT "country total minus dead":
  the prober's roster is not the population of record and covers dufour only, so for jura
  live_nodes is 0 everywhere because nothing measures it -- see liveness_source in
  int_hopr_nodes. Use nodes for geography, live_nodes only alongside an explicit caveat.
*/

SELECT
    network,
  -- as_of_date: the endpoint convention requires a point-in-time endpoint to state WHEN
  -- it is current (check_api_tags.py), otherwise a stale snapshot reads as live -- the
  -- stale-snapshot-caveat lesson. Anchored to the newest on-chain evidence actually in
  -- this snapshot rather than to today(), which would claim freshness the build may not
  -- have. Uncorrelated scalar subquery, matching the api_bridges_kpi_* pattern.
  (
      SELECT toDate(max(greatest(
          coalesce(last_announced_at,        toDateTime(0)),
          coalesce(last_channel_activity_at, toDateTime(0))
      )))
      FROM {{ ref('int_hopr_nodes') }}
  )                                                               AS as_of_date,
    coalesce(nullIf(ip_country, ''), 'UNKNOWN')                     AS country_code,
    if(ip_country IS NULL OR ip_country = '', 0, 1)                 AS geo_resolved,
    count()                                                         AS nodes,
    countIf(is_reachable)                                           AS live_nodes,
    countIf(node_class = 'cover_traffic')                           AS cover_traffic_nodes,
    countIf(node_class = 'gnosisvpn_exit')                          AS gnosisvpn_exit_nodes,
    uniqExact(safe_address)                                         AS distinct_operators,
    uniqExactIf(announced_ip, announced_ip IS NOT NULL)             AS distinct_hosts
FROM {{ ref('int_hopr_nodes') }}
GROUP BY network, country_code, geo_resolved
ORDER BY network, nodes DESC
