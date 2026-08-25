

/*
  Daily network-level snapshot from blokli: live protocol parameters plus account,
  safe and channel counts and balances.

  JURA AND ROTSEE ONLY (no dufour). rotsee is a TESTNET -- is_testnet is exposed so
  its ticket price and balances, which are orders of magnitude away from production,
  cannot be summed into a network total by accident.

  This is the only source of jura economics and of staked/locked balances that needs
  no balance indexing at all, and it is the live counterpart to the oracle events we
  decode from chain: ticket_price_wxhopr and min_ticket_winning_probability here
  should agree with the latest TicketPriceUpdated / WinProbUpdated. Disagreement
  means the decode is stale or an oracle address in contracts_hopr_registry is wrong.

  FORWARD-ONLY. blokli is queried for today; there is no history before the ingestor
  first ran, and none can be recovered. A gap in snapshot_date is a gap forever.

  FINAL is required: ReplacingMergeTree(ingested_at), re-running a day re-inserts it.
*/

SELECT
    -- blokli self-reports DECORATED network names ('jura-prod'), while the on-chain side
    -- -- contracts_hopr_registry_static and every consumer -- uses the bare name ('jura').
    -- Normalizing ONCE here is what keeps the two halves joinable; never re-map in a
    -- consumer. Prefix-safe, so a future 'jura-staging' or 'rotsee-prod' lands on the
    -- bare name instead of becoming its own silent network.
    -- CAST strips LowCardinality first: comparing a LowCardinality(String) to a literal
    -- yields LowCardinality(UInt8), which ClickHouse Cloud rejects outright (code 455).
    multiIf(
        startsWith(CAST(t.network AS String), 'jura'),   'jura',
        startsWith(CAST(t.network AS String), 'rotsee'), 'rotsee',
        CAST(t.network AS String)
    )                                           AS network,
    -- Undecorated API value, kept for audit: the only place 'jura-prod' stays visible.
    -- t.-qualified: the bare name here resolves to the OUTPUT ALIAS above (ClickHouse
    -- alias shadowing, lesson ch-alias-shadows-where).
    CAST(t.network AS String)                   AS raw_network,
    -- Same qualification; equivalent to (normalized network = 'rotsee').
    startsWith(CAST(t.network AS String), 'rotsee') AS is_testnet,
    snapshot_date,
    chain_id,
    block_number,
    api_version,

    ticket_price_wxhopr,
    min_ticket_winning_probability,
    -- HOPR pays relayers probabilistically: a ticket wins with probability p and pays
    -- ticket_price when it does, so the economically meaningful figure -- what a
    -- winning ticket is actually worth -- is price / p. Neither raw column means much
    -- alone, and comparing ticket_price across networks without it is meaningless
    -- because they run different winning probabilities.
    if(min_ticket_winning_probability > 0,
       ticket_price_wxhopr / min_ticket_winning_probability,
       NULL)                                    AS payout_per_winning_ticket_wxhopr,
    key_binding_fee_wxhopr,
    channel_closure_grace_period_s,

    account_count,
    safes_count,
    safes_balance_wxhopr,
    channels_total,
    channels_balance_wxhopr,
    channels_open,
    channels_open_balance_wxhopr,
    channels_pendingtoclose,
    channels_pendingtoclose_balance_wxhopr,
    channels_closed,
    channels_closed_balance_wxhopr
FROM `hopr_db`.`hopr_blokli_network_snapshot` AS t FINAL