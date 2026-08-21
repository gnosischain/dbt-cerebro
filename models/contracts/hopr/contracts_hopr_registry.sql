{{
    config(
        materialized='view',
        tags=['production', 'contracts', 'hopr', 'registry']
    )
}}

/*
  HOPR contract registry -- the address/ABI/network resolution point for every
  downstream HOPR model.

  Why this exists rather than inlining addresses in each event model:

  1. THREE HOPR environments run on Gnosis Chain at the same time. `dufour` is the
     legacy production network (hoprd v2/v3); `jura` is the v4 network that the
     GnosisVPN client defaults to; `rotsee` is the v4 TESTNET. The same logical
     event has a different topic0 on dufour than on the v4 pair, because v4
     repacked the payload (e.g. ChannelBalanceDecreased(bytes32,uint96) on dufour
     vs ChannelBalanceDecreased(bytes32,bytes32) on jura). Filtering by protocol
     instead of by address would silently drop an environment's events.

     rotsee is a testnet: its tickets and balances are not real economics. It is
     carried here so the decode layer is complete and the testnet is analysable,
     and `is_testnet` below is what production marts filter on. Never aggregate it
     with dufour or jura.

  1b. Each v4 environment has been REDEPLOYED once, so a network maps to more than
     one address per contract_type: jura ran 0x69e63a01 (Channels) from 2026-01-06
     until the redeploy at block 47415377 (2026-07-27) moved it to 0x860f50b2, and
     rotsee likewise moved 0x81a79fcd -> 0xd8c91008. Both generations are listed.
     decode_logs filters on contract_type alone, so extra addresses per type are
     picked up automatically -- but only the NEW jura addresses own event_signatures
     rows, so every other row points `abi_source_address` at them (see 2).
     Dropping a generation loses real history: the legacy jura Channels contract
     alone holds ~125k logs, and the two rotsee Channels contracts ~1.13M.

  2. jura's Announcements is an EIP-1967 proxy (implementation
     0x914c4ad36e5ec147be0a94c671d7440f1d0fc8c1), but `abi_source_address` still
     points at the PROXY's own address. That is not an oversight -- proxies need
     no indirection here. fetch_abi_to_csv.py follows the proxy and stores the
     implementation's ABI under the PROXY's contract_address (keeping the impl in
     the separate implementation_address column), so signature_generator.py keys
     every event_signatures row on the proxy address. Verified: 9 signature rows
     are keyed on 0xa91976..., ZERO on 0x914c4ad3....

     `abi_source_address` exists for a different case -- when the ABI genuinely
     lives under some OTHER contract's address, e.g. Circles' shared runtime ABI
     or the Celo Safe singleton. Pointing it at an implementation address makes
     the ABI join miss entirely and yields NULL event names rather than an error,
     so it fails quietly. Do not "fix" this back.

  Addresses come from hoprnet/contracts `ethereum/bindings/contracts-addresses.json`
  and every start_blocktime is the contract's own creation date as observed in
  execution.contracts -- not an estimate.

  HoprChannels 0x81a79fcde8ffe6452e51d8e0493b37c2a5a09c57 was previously excluded
  here as "a v4-shaped pre-jura staging deployment". That identification was wrong:
  it is rotsee's FIRST Channels deployment, i.e. the testnet, confirmed against
  HOPR's own contract registry. It is now included and flagged is_testnet rather
  than dropped -- its ~738k events are real testnet activity, and silently
  discarding them made the testnet invisible instead of excluded.
*/

SELECT
    lower(address)            AS address,
    network,
    -- Derived once here so the on-chain side has the same testnet concept the
    -- blokli staging models already expose, and production marts have a single
    -- flag to filter on. Prefix-safe for the same reason it is there: a future
    -- 'rotsee-2' must not read as production.
    -- CAST strips LowCardinality FIRST: network is LowCardinality(String), and a
    -- predicate over it yields LowCardinality(UInt8), which ClickHouse Cloud
    -- refuses to materialize outright (code 455). Same guard as the blokli
    -- staging models.
    startsWith(CAST(network AS String), 'rotsee') AS is_testnet,
    contract_type,
    lower(abi_source_address) AS abi_source_address,
    toUInt8(is_dynamic)       AS is_dynamic,
    start_blocktime,
    discovery_source
FROM {{ ref('contracts_hopr_registry_static') }}
