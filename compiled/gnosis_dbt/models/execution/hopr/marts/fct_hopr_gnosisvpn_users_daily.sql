

/*
  EVERY AGGREGATE BELOW MUST CARRY A DAY PREDICATE. This is a correctness
  requirement, not a style choice -- read before adding a measure.

  The spine LEFT JOINs the activity stream over a RANGE (day BETWEEN date-30 AND
  date). ClickHouse fills an unmatched row with the column TYPE DEFAULT, so a day
  with no client activity yields node_address = '' and day = 1970-01-01, and a
  bare uniqExact(a.node_address) would count that empty string as ONE distinct
  client -- every quiet day reporting 1 active user instead of 0, most visibly at
  the start of the series where the product genuinely had none.

  What makes it safe is that every uniqExactIf here tests `a.day >= ...` or
  `a.day = ...`, and the 1970 sentinel fails all of them, so the phantom row is
  never counted. join_use_nulls would express this more directly, but ClickHouse
  rejects it outright on a range join (code 403, INVALID_JOIN_ON_EXPRESSION):
  a join condition spanning both tables is unsupported with that setting. Hence
  the predicates carry the guarantee instead.

  A future measure written as uniqExact(a.node_address) with no day condition
  would reintroduce the bug silently. Don't.
*/

/*
  GnosisVPN client activity per network per day, on a dense date spine.

  This is the DEMAND side of GnosisVPN. Its sibling
  api_gnosisvpn_exit_activity_daily measures the supply side (traffic per named
  exit); neither substitutes for the other.

  TWO DEFINITIONS ARE PUBLISHED SIDE BY SIDE, on purpose.

  1. active_users_30d / active_users_7d -- HOPR-COMPARABLE. Mirrors the SQL behind
     HOPR's own GnosisVPN dashboard: a client is active in a window if it redeemed
     a ticket as a channel source OR first key-bound (deployed) inside it. Use
     this, and only this, when comparing against numbers HOPR publishes.

  2. using_users_30d / using_users_7d -- STRICTER, ours. Ticket redemptions only,
     dropping the deployment half. A first key-binding proves someone installed
     the client, not that they routed traffic through it, so definition 1 counts
     installs as usage on the day they happen.

     THE GAP BETWEEN THE TWO IS THE POINT: active minus using is the
     deployed-but-never-used cohort. Quote definition 1 for comparability and
     definition 2 for whether the product is being used, and never mix them in one
     series.

  WINDOW BOUNDS ARE COPIED, INCLUDING AN ASYMMETRY. HOPR's query filters the 30d
  window as `day BETWEEN d - 30 AND d` (31 days) and the 7d window as
  `day BETWEEN d - 6 AND d` (7 days). That is almost certainly an off-by-one in
  the 30d leg, but it is reproduced exactly here: a metric advertised as
  comparable has to be comparable, and silently "fixing" it would put us
  permanently ~1 day of cohort above their published figure with no way for a
  reader to tell why. Documented, not corrected.

  NO BYTES / NO GB. HOPR's dashboard converts redemptions into an estimated
  volume. A redeemed ticket is a PROBABILISTIC payment -- it is proportional to
  relayed traffic in expectation, not a measurement of it -- and real byte counts
  exist only in node-local Prometheus metrics that are not public. A GB column
  here would be a constant multiplied by a ticket count wearing a bandwidth
  label, so it is deliberately absent. Do not add one.

  rotsee is excluded: it is the v4 testnet and its clients are test deployments.
*/

WITH clients AS (
    SELECT
        network,
        node_address,
        first_deploy_date
    FROM `dbt`.`int_hopr_vpn_clients`
    WHERE NOT is_testnet
),

-- Redemptions attributable to a client, i.e. where the client is the channel
-- SOURCE (the party paying to have traffic relayed). A client appearing as
-- destination would be earning as a relay, which by construction it is not.
redemptions AS (
    SELECT
        e.network                                   AS network,
        toDate(e.block_timestamp)                   AS day,
        e.source_node                               AS node_address
    FROM `dbt`.`int_hopr_channels_events` AS e
    INNER JOIN clients AS c
        ON c.network = e.network AND c.node_address = e.source_node
    WHERE e.event_name = 'TicketRedeemed'
),

-- The deployment leg: one row per client, on the day it first key-bound.
deployments AS (
    SELECT network, first_deploy_date AS day, node_address
    FROM clients
),

activity AS (
    SELECT network, day, node_address, 1 AS is_usage FROM redemptions
    UNION ALL
    SELECT network, day, node_address, 0 AS is_usage FROM deployments
),

-- Dense spine so a quiet day is a zero rather than a gap: a rolling window read
-- off a sparse series silently shortens itself around missing days.
bounds AS (
    SELECT network, min(day) AS first_day, today() AS last_day
    FROM activity GROUP BY network
),

date_spine AS (
    SELECT network, first_day + toUInt32(n) AS date
    FROM bounds
    ARRAY JOIN range(toUInt32(last_day - first_day) + 1) AS n
),

-- Per-day volume, independent of the rolling user counts.
daily_volume AS (
    SELECT
        e.network                                                       AS network,
        toDate(e.block_timestamp)                                       AS date,
        countIf(e.event_name = 'TicketRedeemed')                        AS tickets_redeemed,
        -- Value rides on ChannelBalanceDecreased, NOT on TicketRedeemed: the
        -- decrease is the payout. Counting tickets on one event and summing value
        -- on the other is correct, not a mismatch.
        toDecimal128(sumIf(coalesce(e.redeemed_wei, toUInt256(0)),
                           e.event_name = 'ChannelBalanceDecreased') / 1e18, 18) AS redeemed_wxhopr,
        uniqExact(e.channel_id)                                         AS channels_active
    FROM `dbt`.`int_hopr_channels_events` AS e
    INNER JOIN clients AS c
        ON c.network = e.network AND c.node_address = e.source_node
    GROUP BY network, date
)

SELECT
    s.network                                                           AS network,
    s.date                                                              AS date,

    -- Definition 1 -- HOPR-comparable (redemption OR deployment).
    uniqExactIf(a.node_address, a.day >= s.date - 30)                   AS active_users_30d,
    uniqExactIf(a.node_address, a.day >= s.date - 6)                    AS active_users_7d,
    uniqExactIf(a.node_address, a.day = s.date)                         AS active_users_1d,

    -- Definition 2 -- usage only (redemption).
    uniqExactIf(a.node_address, a.is_usage = 1 AND a.day >= s.date - 30) AS using_users_30d,
    uniqExactIf(a.node_address, a.is_usage = 1 AND a.day >= s.date - 6)  AS using_users_7d,
    uniqExactIf(a.node_address, a.is_usage = 1 AND a.day = s.date)       AS using_users_1d,

    -- Cohort movement.
    uniqExactIf(a.node_address, a.is_usage = 0 AND a.day = s.date)      AS new_clients,
    max(coalesce(v.tickets_redeemed, 0))                                AS tickets_redeemed,
    max(coalesce(v.redeemed_wxhopr, toDecimal128(0, 18)))               AS redeemed_wxhopr,
    max(coalesce(v.channels_active, 0))                                 AS channels_active
FROM date_spine AS s
LEFT JOIN activity AS a
    ON a.network = s.network
   AND a.day BETWEEN s.date - 30 AND s.date
LEFT JOIN daily_volume AS v
    ON v.network = s.network AND v.date = s.date
GROUP BY s.network, s.date
ORDER BY s.network, s.date