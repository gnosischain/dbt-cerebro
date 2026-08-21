{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(network, activity_date)',
    tags=['production','hopr','fct']
  )
}}

/*
  Daily HOPR payment-channel activity per network, split by whether the traffic
  is HOPR's own cover traffic or not.

  THE SPLIT IS THE POINT OF THIS MODEL. On dufour, nearly every ChannelOpened
  event originates from the ten cover-traffic nodes that HOPR publishes in
  ct-research (ct-app/.configs/core_prod_config.yaml) -- it has been the
  overwhelming majority in every year dufour has been live. Cover
  traffic is HOPR paying node runners in proportion to stake -- a staking rewards
  programme -- and the config's `channel.max_age_seconds: 172800` means every
  channel is torn down and reopened every 48 hours, which is what generates
  almost all of the observed churn. Reporting these columns summed together
  would present a rewards distribution schedule as user demand.

  jura carries no cover traffic at all (verified: zero events involving any of
  the ten addresses), so its totals are real session traffic.

  `*_wxhopr` columns are derived from balance diffs, because neither network ever
  emits an amount -- see int_hopr_channels_events.
*/

WITH ev AS (
    SELECT
        e.network                                          AS network,
        toDate(e.block_timestamp)                          AS activity_date,
        e.event_name                                       AS event_name,
        e.channel_id                                       AS channel_id,
        e.source_node                                      AS source_node,
        e.destination_node                                 AS destination_node,
        e.redeemed_wei                                     AS redeemed_wei,
        e.balance_delta_wei                                AS balance_delta_wei,
        -- Classify on the source: cover traffic is defined by who PAYS, i.e. who
        -- opens and funds the channel, not by who receives.
        -- CAST(... AS String) strips LowCardinality. node_class is
        -- LowCardinality(String) and that propagates through the `= 'cover_traffic'`
        -- comparison below, making is_cover_traffic a LowCardinality(UInt8) -- a type
        -- ClickHouse refuses to materialize (code 455,
        -- SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY). Verified: only CAST strips it;
        -- toString() and materialize() both PRESERVE LowCardinality here.
        CAST(coalesce(src.node_class, 'unclassified') AS String) AS source_class
    FROM {{ ref('int_hopr_channels_events') }} AS e
    LEFT JOIN {{ ref('hopr_node_registry') }}  AS src
        ON e.source_node = lower(src.node_address)
    -- Production networks only. rotsee is the v4 TESTNET: its channels and tickets
    -- outnumber jura's several times over, so leaving it in would swamp every
    -- headline with traffic that is not real economics. It stays queryable in
    -- int_hopr_channels_events, which keeps the flag rather than the filter.
    WHERE NOT e.is_testnet
)

SELECT
    network,
    activity_date,
    toUInt8(source_class = 'cover_traffic')                                      AS is_cover_traffic,

    countIf(event_name = 'ChannelOpened')                                        AS channels_opened,
    countIf(event_name = 'ChannelClosed')                                        AS channels_closed,
    countIf(event_name = 'OutgoingChannelClosureInitiated')                      AS closures_initiated,
    countIf(event_name = 'TicketRedeemed')                                       AS tickets_redeemed,
    countIf(event_name = 'ChannelBalanceIncreased')                              AS fundings,

    -- Value redeemed by relayers, and value pushed into channels. Both are
    -- reconstructed from balance diffs; rows without both diff endpoints observed
    -- contribute nothing rather than a wrong zero.
    toDecimal128(sum(coalesce(redeemed_wei, toUInt256(0))) / 1e18, 18)           AS redeemed_wxhopr,
    toDecimal128(sumIf(coalesce(balance_delta_wei, toInt256(0)),
                       event_name = 'ChannelBalanceIncreased'
                       AND balance_delta_wei > 0) / 1e18, 18)                    AS funded_wxhopr,

    uniqExact(channel_id)                                                        AS active_channels,
    uniqExactIf(source_node, source_node IS NOT NULL)                            AS unique_source_nodes,
    uniqExactIf(destination_node, destination_node IS NOT NULL)                  AS unique_destination_nodes,
    count()                                                                      AS events
FROM ev
GROUP BY network, activity_date, is_cover_traffic
