

/*
  Serving view over fct_hopr_network_daily: daily payment-channel activity per network,
  SPLIT BY is_cover_traffic.

  THE SPLIT IS THE CONTRACT, NOT A CONVENIENCE. On dufour nearly every channel event
  originates from the ten cover-traffic nodes HOPR publishes in ct-research -- cover
  traffic is HOPR paying node runners in proportion to stake, i.e. a staking rewards
  programme, and the config's 48-hour channel max age is what generates almost all
  observed churn. Summing across is_cover_traffic presents a rewards schedule as user
  demand. Any consumer must split or filter on it; a total is not a usable number.

  tickets_redeemed is the trustworthy volume measure. channels_opened is not comparable
  across years, because the cover-traffic share of it has risen sharply since launch.
*/

SELECT
    network,
    -- Named `date`, not `activity_date`: the endpoint convention requires a daily
    -- endpoint to expose its grain as block_date/date/day (check_api_tags.py), and it
    -- keeps this consistent with api_hopr_network_health_daily so the two can be joined
    -- on (network, date) without a rename.
    activity_date                                                   AS date,
    is_cover_traffic,
    channels_opened,
    channels_closed,
    closures_initiated,
    tickets_redeemed,
    fundings,
    redeemed_wxhopr,
    funded_wxhopr,
    active_channels,
    unique_source_nodes,
    unique_destination_nodes,
    events
FROM `dbt`.`fct_hopr_network_daily`
WHERE activity_date < today()
ORDER BY network, date, is_cover_traffic