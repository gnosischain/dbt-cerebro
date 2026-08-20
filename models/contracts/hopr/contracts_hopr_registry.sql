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

  1. Two HOPR networks run on Gnosis Chain at the same time. `dufour` is the
     legacy production network (hoprd v2/v3); `jura` is the v4 network that the
     GnosisVPN client defaults to, live since block 47415377 (2026-07-27). The
     same logical event has a different topic0 on each, because v4 repacked the
     payload (e.g. ChannelBalanceDecreased(bytes32,uint96) on dufour vs
     ChannelBalanceDecreased(bytes32,bytes32) on jura). Filtering by protocol
     instead of by address would silently drop one network's events.

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

  Deliberately EXCLUDED: HoprChannels 0x81a79fcde8ffe6452e51d8e0493b37c2a5a09c57.
  It is a v4-shaped pre-jura staging deployment that generated a large burst of
  events and then stopped dead two days after jura went live. The volume makes it
  look production-sized; it is not, and it must never be aggregated with either
  network.
*/

SELECT
    lower(address)            AS address,
    network,
    contract_type,
    lower(abi_source_address) AS abi_source_address,
    toUInt8(is_dynamic)       AS is_dynamic,
    start_blocktime,
    discovery_source
FROM {{ ref('contracts_hopr_registry_static') }}
