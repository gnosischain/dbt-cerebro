{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(network, channel_id, block_number, log_index)',
    partition_by='toStartOfYear(block_timestamp)',
    settings={ 'allow_nullable_key': 1 },
    tags=['dev','hopr','intermediate'],
    pre_hook=["SET allow_experimental_json_type = 1"],
    post_hook=["SET allow_experimental_json_type = 0"]
  )
}}

/*
  Normalized HOPR payment-channel events across BOTH live networks.

  Materialized as a table, not an incremental append, on purpose: the expensive
  work (scanning execution.logs) already happened in
  contracts_hopr_Channels_events. This model reads only that small decoded table
  (~2M rows), so a full rebuild is cheap and it sidesteps the append-watermark
  hazard class entirely (logs landing below a high-water mark never being
  decoded, and appending over a populated partition duplicating rows).

  Three things this model reconciles:

  1. DIFFERENT EVENT SHAPES PER NETWORK. dufour emits typed scalars
     (newBalance uint96, newTicketIndex uint48, closureTime uint32). jura emits
     the raw packed storage word as `channel` bytes32, because
     HoprChannels._channelState() returns `sload(...)` directly. Solidity packs
     struct fields from the least-significant bit upward in declaration order:

        Channel { balance uint96 | ticketIndex uint48 | closureTime uint32
                  | epoch uint24 | status uint8 }

     => balance = bits 0..95, ticketIndex = 96..143, closureTime = 144..175,
        epoch = 176..199, status = 200..207, and bits >=208 are always zero.

     This layout is VERIFIED, not inferred: unpacked balances match blokli's
     GraphQL API (blokli.jura.hoprnet.link) to the exact wei for channels whose
     last event is their current state.

     CLICKHOUSE TRAP: do not write the masks as decimal literals. 2^96-1 exceeds
     Int64 and is parsed as Float64, silently rounding UP to 2^96, so
     `bitAnd(w, 79228162514264337593543950335)` returns impossible values. Build
     masks with bitShiftLeft(toUInt256(1), n) - 1.

  2. dufour's ChannelOpened CARRIES NO channelId -- only (source, destination).
     channelId is keccak256(abi.encodePacked(source, destination)), so it is
     derived here. Verified against real data: for all 52 dufour ChannelOpened
     events in blocks 47400000-47492863, the derived id equals the channelId
     topic of the sibling ChannelBalanceIncreased in the same transaction.
     Because the id is a pure function of the pair, channel_id <-> (source,
     destination) is a bijection and stays invariant across channel reopens
     (only `epoch` increments), which is what makes the fill in step 3 sound.

  3. SOURCE/DESTINATION ARE ONLY ON ChannelOpened. Every other event identifies
     the channel solely by channelId, so the endpoints are filled across the
     whole (network, channel_id) partition. This also recovers endpoints for
     events whose ChannelOpened predates our decode window.

  NOT a ticket count: `ticket_index` advances by a ticket's index RANGE, not by
  one per ticket (jura channels show indices in the millions within a week, and
  blokli agrees). Never aggregate it as a volume.
*/

WITH ev AS (
    SELECT
        r.network                                   AS network,
        e.block_number                              AS block_number,
        e.block_timestamp                           AS block_timestamp,
        e.transaction_hash                          AS transaction_hash,
        e.log_index                                 AS log_index,
        lower(e.contract_address)                   AS contract_address,
        e.event_name                                AS event_name,
        e.decoded_params                            AS p
    FROM {{ ref('contracts_hopr_Channels_events') }} AS e
    INNER JOIN {{ ref('contracts_hopr_registry') }}  AS r
        ON lower(e.contract_address) = replaceAll(r.address, '0x', '')
    WHERE r.contract_type = 'Channels'
      -- Channel lifecycle only. DomainSeparatorUpdated / LedgerDomainSeparatorUpdated
      -- are contract-config events with no channel_id and would produce orphan rows.
      AND e.event_name IN (
            'ChannelOpened', 'ChannelBalanceIncreased', 'ChannelBalanceDecreased',
            'TicketRedeemed', 'OutgoingChannelClosureInitiated', 'ChannelClosed'
          )
),

typed AS (
    SELECT
        network,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        event_name,

        -- channelId: present on every jura event and on all dufour events
        -- except ChannelOpened, which must be derived from the endpoint pair.
        lower(if(
            empty(coalesce(p['channelId'], '')),
            concat('0x', hex(keccak256(unhex(concat(
                replaceAll(lower(coalesce(p['source'], '')),      '0x', ''),
                replaceAll(lower(coalesce(p['destination'], '')), '0x', '')
            ))))),
            coalesce(p['channelId'], '')
        ))                                                       AS channel_id,

        nullIf(lower(coalesce(p['source'], '')), '')              AS source_node_raw,
        nullIf(lower(coalesce(p['destination'], '')), '')         AS destination_node_raw,

        -- jura only: the packed storage word, as UInt256
        if(
            empty(coalesce(p['channel'], '')),
            toUInt256(0),
            reinterpretAsUInt256(reverse(unhex(replaceAll(coalesce(p['channel'], ''), '0x', ''))))
        )                                                        AS packed_word,
        NOT empty(coalesce(p['channel'], ''))                    AS is_packed,

        nullIf(coalesce(p['newBalance'], ''), '')                 AS dufour_new_balance,
        nullIf(coalesce(p['newTicketIndex'], ''), '')             AS dufour_new_ticket_index,
        nullIf(coalesce(p['closureTime'], ''), '')                AS dufour_closure_time
    FROM ev
),

unpacked AS (
    SELECT
        network,
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        contract_address,
        event_name,
        channel_id,
        source_node_raw,
        destination_node_raw,

        -- Balance after the event, in wei. NULL means "this event carries no
        -- balance observation" (dufour ChannelOpened and ChannelClosed), which
        -- must stay NULL so the delta window below does not diff against a
        -- fabricated zero.
        multiIf(
            is_packed,               toNullable(toUInt256(bitAnd(packed_word, bitShiftLeft(toUInt256(1), 96) - 1))),
            dufour_new_balance IS NOT NULL, toNullable(toUInt256(dufour_new_balance)),
            NULL
        )                                                        AS balance_wei,

        multiIf(
            is_packed,               toNullable(toUInt64(bitAnd(bitShiftRight(packed_word, 96), bitShiftLeft(toUInt256(1), 48) - 1))),
            dufour_new_ticket_index IS NOT NULL, toNullable(toUInt64(dufour_new_ticket_index)),
            NULL
        )                                                        AS ticket_index,

        multiIf(
            is_packed,               toNullable(toUInt32(bitAnd(bitShiftRight(packed_word, 144), bitShiftLeft(toUInt256(1), 32) - 1))),
            dufour_closure_time IS NOT NULL, toNullable(toUInt32(dufour_closure_time)),
            NULL
        )                                                        AS closure_time,

        -- epoch and status exist only in the packed jura payload. On dufour they
        -- are not emitted at all; leaving them NULL is honest, whereas defaulting
        -- to 0 would read as ChannelStatus.CLOSED.
        if(is_packed, toNullable(toUInt32(bitAnd(bitShiftRight(packed_word, 176), bitShiftLeft(toUInt256(1), 24) - 1))), NULL) AS epoch,
        if(is_packed, toNullable(toUInt8(bitAnd(bitShiftRight(packed_word, 200), toUInt256(255)))), NULL)                      AS status_code,

        -- Guard on the layout itself: bits >= 208 must always be zero. A non-zero
        -- value here means the struct changed and the offsets above are stale.
        if(is_packed, toNullable(toUInt256(bitShiftRight(packed_word, 208))), NULL)                                            AS packing_overflow_check
    FROM typed
),

-- channel_id <-> (source, destination) is a bijection, so the endpoints can be
-- filled across the whole partition from whichever event carried them.
filled AS (
    SELECT
        u.* EXCEPT (source_node_raw, destination_node_raw),
        max(u.source_node_raw)      OVER (PARTITION BY u.network, u.channel_id) AS source_node,
        max(u.destination_node_raw) OVER (PARTITION BY u.network, u.channel_id) AS destination_node
    FROM unpacked AS u
),

-- Neither network emits an amount: every balance-bearing event reports the NEW
-- balance. Redeemed / funded value is therefore the diff against the previous
-- balance-bearing event in the same channel.
delta AS (
    SELECT
        f.*,
        anyLast(f.balance_wei) OVER (
            PARTITION BY f.network, f.channel_id
            ORDER BY f.block_number, f.log_index
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        )                                                        AS prev_balance_wei
    FROM filled AS f
)

SELECT
    network,
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    contract_address,
    event_name,
    channel_id,
    source_node,
    destination_node,
    balance_wei,
    prev_balance_wei,
    toUInt8(balance_wei IS NOT NULL AND prev_balance_wei IS NOT NULL)  AS has_balance_delta,

    -- Signed delta in wei, only where both endpoints of the diff are observed.
    if(balance_wei IS NOT NULL AND prev_balance_wei IS NOT NULL,
       toNullable(toInt256(balance_wei) - toInt256(prev_balance_wei)),
       NULL)                                                          AS balance_delta_wei,

    -- Value actually paid out to the destination node on a ticket redemption.
    -- Scoped to ChannelBalanceDecreased because that is the only event whose
    -- decrease is a payout; a channel closing also drops the balance but returns
    -- funds to the source instead.
    if(event_name = 'ChannelBalanceDecreased'
         AND balance_wei IS NOT NULL AND prev_balance_wei IS NOT NULL
         AND prev_balance_wei > balance_wei,
       toNullable(toUInt256(prev_balance_wei - balance_wei)),
       NULL)                                                          AS redeemed_wei,

    ticket_index,
    closure_time,
    epoch,
    status_code,
    multiIf(status_code IS NULL, 'unknown',
            status_code = 0, 'CLOSED',
            status_code = 1, 'OPEN',
            status_code = 2, 'PENDING_TO_CLOSE',
            'unexpected')                                             AS channel_status,
    packing_overflow_check
FROM delta
