

/*
  Serving view over fct_hopr_network_health_daily: nodes ever registered on-chain versus
  nodes actually up, per network per day.

  THE TWO COLUMNS ARE NOT INTERCHANGEABLE AND THAT IS THE POINT. Registration is
  cumulative and never expires, so nodes_registered_cumulative counts machines that ran
  once in 2023 and vanished; nodes_online_avg is the live network. They have diverged
  every year. A consumer that labels either one "network size" without saying which is
  wrong, and the registry version is wrong by several times over.

  hours_observed is published deliberately: the upstream hourly history is missing a
  substantial share of its hours, so a day can rest on anything from one observation to
  twenty-four. Read nodes_online_avg with it and flag or drop thin days.

  NULL in any prober column means unmeasured, never zero nodes. Prober coverage is
  dufour-only (never ported to jura/v4) and starts when the click-runner ingestor first
  ran, so jura rows carry registration counts and nothing else.
*/

SELECT
    network,
    date,
    nodes_registered_new,
    nodes_registered_cumulative,
    nodes_online_avg,
    nodes_online_min,
    nodes_online_max,
    hours_observed,
    nodes_probed,
    nodes_reachable,
    nodes_high_availability,
    avg_latency_ms,
    p50_latency_ms,
    avg_availability_24h
FROM `dbt`.`fct_hopr_network_health_daily`
WHERE date < today()
ORDER BY network, date