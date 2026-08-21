

/*
  SERVED AT tier3. GnosisVPN is still in closed beta (El Dorado; early access
  lands with Shangri-La, Sept 2026), so tier3 is the honest shelf for it: a real
  endpoint contract, marked as the least settled tier. Every caveat below travels
  with the data and is not optional context.

  This is the SUPPLY side -- traffic arriving at named exits. The demand side
  (how many clients, how many actually used it) is api_hopr_gnosisvpn_users_daily.
  Reading either as "GnosisVPN usage" on its own will mislead.
*/

/*
  GnosisVPN traffic by exit node and country, per day.

  This is the one HOPR series that is genuinely about product demand rather than
  protocol plumbing. It works because the GnosisVPN client's exit-node roster is
  committed to git (gnosis/gnosis_vpn -> linux/resources/config-jura.toml) with
  an address, city and country per destination, so anonymous channel activity can
  be attributed to a named exit.

  Scoped to jura deliberately: jura is the network the GnosisVPN client defaults
  to, and it carries no cover traffic, so these tickets are real relayed sessions.

  Two honest caveats a consumer of this model has to carry:

  - TICKETS, NOT BYTES. A redeemed ticket is a probabilistic payment, so ticket
    counts are proportional to relayed volume in expectation but are not a
    bandwidth measurement. Actual bytes exist only in node-local Prometheus
    metrics, which are not public.

  - CHANNEL COUNTS ARE NOT DEMAND. The client auto-maintains channels toward
    `target_open_channels` (gnosis_vpn-client hopr/strategy_config.rs), so
    channels_opened tracks how many clients are running, not how much they used.
    Read tickets_redeemed / redeemed_wxhopr for usage.

  The city/country labels are the product's own metadata. They are NOT
  independently verified geography: the announced multiaddresses for these nodes
  cluster in a single 185.9.1.0/24 block plus Google Cloud ranges.
*/

WITH ev AS (
    SELECT
        toDate(e.block_timestamp)   AS date,
        e.event_name                AS event_name,
        e.channel_id                AS channel_id,
        e.source_node               AS source_node,
        e.redeemed_wei              AS redeemed_wei,
        x.label                     AS exit_label,
        x.location_city             AS location_city,
        x.country_code              AS country_code
    FROM `dbt`.`int_hopr_channels_events` AS e
    INNER JOIN `dbt`.`hopr_node_registry` AS x
        ON e.destination_node = lower(x.node_address)
       AND x.node_class = 'gnosisvpn_exit'
    WHERE e.network = 'jura'
)

SELECT
    -- Named `date`, not `activity_date`: check_api_tags requires a served
    -- granularity:daily endpoint to expose its grain as date/block_date/day.
    date,
    country_code,
    location_city,
    exit_label,
    countIf(event_name = 'TicketRedeemed')                                AS tickets_redeemed,
    toDecimal128(sum(coalesce(redeemed_wei, toUInt256(0))) / 1e18, 18)    AS redeemed_wxhopr,
    countIf(event_name = 'ChannelOpened')                                 AS channels_opened,
    countIf(event_name = 'ChannelClosed')                                 AS channels_closed,
    -- Distinct counterparties funding channels toward this exit: an upper bound
    -- on running clients, not a user count (one operator can run many nodes).
    uniqExactIf(source_node, source_node IS NOT NULL)                     AS unique_counterparty_nodes,
    uniqExact(channel_id)                                                 AS channels_seen
FROM ev
GROUP BY date, country_code, location_city, exit_label