

/*
  Current nodes aggregated to a plottable point: one row per (network, country, city) with
  coordinates and a node count. Built for a geo map, which is why it is shaped differently
  from api_hopr_nodes_by_country_latest -- that one keeps unresolved nodes as a row, this
  one CANNOT, because a point with no coordinates is not plottable.

  SO THIS VIEW IS DELIBERATELY INCOMPLETE, and a map built on it alone silently under-counts
  the network. Nodes without coordinates are excluded here and counted in
  api_hopr_nodes_by_country_latest under 'UNKNOWN'. Any map card must publish that residual
  next to it, or a reader will take the plotted total as the network size.

  Coordinates come from ipinfo's city centroid, not the machine: every node in a city
  collapses to the same point, which is why nodes is an aggregate rather than one row per
  node. City-level is also the honest resolution -- IP geolocation does not locate a host
  more precisely than that, and pretending otherwise invites reading a datacenter's
  registered address as a node's whereabouts.
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
      FROM `dbt`.`int_hopr_nodes`
  )                                                               AS as_of_date,
    ip_country                                                      AS country_code,
    coalesce(nullIf(ip_city, ''), 'UNKNOWN_CITY')                   AS city,
    round(avg(ip_latitude), 4)                                      AS latitude,
    round(avg(ip_longitude), 4)                                     AS longitude,
    count()                                                         AS nodes,
    countIf(is_reachable)                                           AS live_nodes,
    uniqExact(announced_ip)                                         AS distinct_hosts,
    uniqExact(safe_address)                                         AS distinct_operators,
    countIf(node_class = 'cover_traffic')                           AS cover_traffic_nodes,
    countIf(node_class = 'gnosisvpn_exit')                          AS gnosisvpn_exit_nodes,
    -- Dominant provider at this point, so a map tooltip can say who actually hosts it.
    topK(1)(hosting_provider)[1]                                    AS top_hosting_provider
FROM `dbt`.`int_hopr_nodes`
WHERE ip_latitude IS NOT NULL
  AND ip_longitude IS NOT NULL
  AND ip_country IS NOT NULL
GROUP BY network, country_code, city
ORDER BY network, nodes DESC