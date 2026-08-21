

/*
  Per-node identity snapshot from blokli, HOPR's own v4 indexer.

  JURA AND ROTSEE ONLY -- blokli does not serve dufour, so this is the mirror image
  of the prober feed's coverage. rotsee is a TESTNET: its nodes, balances and ticket
  economics are not production and must be filtered out of any network metric.

  THIS IS A CROSS-CHECK, NOT AN INGESTION SOURCE. Measured against the on-chain
  jura node set built by int_hopr_nodes, blokli returns no node we do not already
  have, disagrees on no IP, and would contribute a single additional address. So
  int_hopr_nodes deliberately does NOT read it -- adding it would buy ~one row while
  making the announcement decode's correctness depend on a third-party indexer.
  Its value is exactly that agreement: it independently confirms the Announcements
  decode, and it carries packet_key/keyid, which exist nowhere on-chain.

  FINAL is required: ReplacingMergeTree(ingested_at), re-running a day re-inserts it.
*/

SELECT
    -- blokli self-reports DECORATED network names ('jura-prod' on the snapshot feed),
    -- while the on-chain side -- contracts_hopr_registry_static and every consumer --
    -- uses the bare name ('jura'). This feed happens to serve the bare name today, so
    -- the mapping is a no-op here, but it must stay identical to the snapshot model's:
    -- normalize ONCE in staging, never re-map in a consumer. Prefix-safe, so a future
    -- 'jura-staging' lands on the bare name instead of becoming its own silent network.
    -- CAST strips LowCardinality first: comparing a LowCardinality(String) to a literal
    -- yields LowCardinality(UInt8), which ClickHouse Cloud rejects outright (code 455).
    multiIf(
        startsWith(CAST(network AS String), 'jura'),   'jura',
        startsWith(CAST(network AS String), 'rotsee'), 'rotsee',
        CAST(network AS String)
    )                                           AS network,
    -- Undecorated API value, kept for audit and to make a future decoration visible.
    CAST(network AS String)                     AS raw_network,
    -- Equivalent to (normalized network = 'rotsee'), written against the source column
    -- rather than the output alias above -- an alias of the same name shadows the source
    -- column and makes which one is being read ambiguous.
    startsWith(CAST(network AS String), 'rotsee') AS is_testnet,
    snapshot_date,
    keyid,
    -- Named node_address to match every other HOPR model; blokli calls it chain_key.
    chain_key                                   AS node_address,
    packet_key,
    safe_address,
    multiaddress,
    multiaddress_count,
    -- multiaddress is a libp2p path, e.g. /ip4/185.9.1.97/udp/9091/quic-v1 -- element
    -- 3 once split on '/' (leading empty element is 1). Guarded on the /ip4/ prefix so
    -- a /ip6/ or /dns4/ address yields NULL instead of a fragment that would look like
    -- an IPv4 address to ipinfo and silently fail to enrich.
    if(startsWith(multiaddress, '/ip4/'), splitByChar('/', multiaddress)[3], NULL) AS announced_ip
FROM `hopr_db`.`hopr_blokli_nodes` FINAL