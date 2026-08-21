

/*
  Current node counts per network per hosting provider, plus the host-concentration
  numbers that make "decentralised" checkable.

  'Unknown' AND 'UNRESOLVED' ARE REAL BUCKETS. 'Unknown' comes from the shared
  stg_crawlers_data__ipinfo classifier and means the network's org string matched no rule
  -- it is NOT residential and must not be folded into either side of a home-vs-datacenter
  split (see that model's generic_provider doc for why the classifier refuses to guess).
  'UNRESOLVED' is a node with no ipinfo row at all. Both are published so a chart cannot
  quietly compute shares over a subset.

  distinct_hosts is the number the node count flatters. Nodes routinely share an IP, so
  nodes / distinct_hosts is how much a machine count differs from a node count -- the more
  honest read of concentration than either alone.
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
    multiIf(
        hosting_provider IS NOT NULL, hosting_provider,
        announced_ip IS NULL,         'NO_IPV4',
        'UNRESOLVED'
    )                                                               AS hosting_provider,
    count()                                                         AS nodes,
    countIf(is_reachable)                                           AS live_nodes,
    uniqExactIf(announced_ip, announced_ip IS NOT NULL)             AS distinct_hosts,
    uniqExact(safe_address)                                         AS distinct_operators,
    countIf(node_class = 'cover_traffic')                           AS cover_traffic_nodes,
    countIf(node_class = 'gnosisvpn_exit')                          AS gnosisvpn_exit_nodes
FROM `dbt`.`int_hopr_nodes`
GROUP BY network, hosting_provider
ORDER BY network, nodes DESC