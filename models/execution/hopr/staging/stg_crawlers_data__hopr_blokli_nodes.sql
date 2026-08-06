{{
    config(
        materialized='view',
        tags=['production','staging','crawlers_data','hopr']
    )
}}

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
    network,
    -- CAST strips LowCardinality first: comparing a LowCardinality(String) to a literal
    -- yields LowCardinality(UInt8), which ClickHouse Cloud rejects outright (code 455).
    CAST(network AS String) = 'rotsee'          AS is_testnet,
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
FROM {{ source('crawlers_data_hopr', 'hopr_blokli_nodes') }} FINAL
