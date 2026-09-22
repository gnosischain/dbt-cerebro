

/*
  Normalized HOPR payment-channel events across BOTH live networks.

  Materialized as a table, not an incremental append, on purpose: the expensive
  work (scanning execution.logs) already happened in
  contracts_hopr_Channels_events. This model reads only that decoded table, so a
  full rebuild stays bounded and it sidesteps the append-watermark hazard class
  entirely (logs landing below a high-water mark never being decoded, and
  appending over a populated partition duplicating rows).

  That source is 5.2M rows as of 2026-09, not the ~2M this header used to claim,
  so "cheap" is no longer the word. Re-measure before assuming a table rebuild is
  still the right trade.

  WHY THE query_settings ARE WHAT THEY ARE. Measured 2026-09-22, same data, same
  server: the rebuild peaked at 3.68 GiB and was repeatedly killed by the
  OvercommitTracker at 2.4-2.8 GiB because the replica had under 2 GiB free. Two
  changes took it to 1.79 GiB / 26s, byte-identical output (5,158,181 rows,
  cityHash64 fingerprint 6100965447899039257 at the 2026-09-21 12:59:35 cutoff):

    * the single-scope window below (two sorts -> one)
    * max_threads 2 -> 1, spill thresholds 500MB -> 100MB, and a bounded
      insert block (262144 rows) so the write side stops buffering whole parts

  max_threads=1 costs ~10s of wall clock and is the point: two threads means two
  sort buffers. Do not raise these back without re-measuring peak memory.

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
        -- Carried from the registry rather than re-derived, so the testnet rule is
        -- defined once. rotsee rows are kept here on purpose -- the testnet stays
        -- analysable at the int layer and is filtered at the fct/api layer, the same
        -- split the blokli staging models use.
        r.is_testnet                                AS is_testnet,
        e.block_number                              AS block_number,
        e.block_timestamp                           AS block_timestamp,
        e.transaction_hash                          AS transaction_hash,
        e.log_index                                 AS log_index,
        lower(e.contract_address)                   AS contract_address,
        e.event_name                                AS event_name,
        e.decoded_params                            AS p
    FROM `dbt`.`contracts_hopr_Channels_events` AS e
    INNER JOIN `dbt`.`contracts_hopr_registry`  AS r
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
        is_testnet,
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
        is_testnet,
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

-- Two window computations, ONE scope, on purpose. DO NOT split these back into
-- separate CTEs for readability:
--
--   * `source_node`/`destination_node` -- channel_id <-> (source, destination) is a
--     bijection, so the endpoints are filled across the whole partition from whichever
--     event carried them.
--   * `prev_balance_wei` -- neither network emits an amount; every balance-bearing event
--     reports the NEW balance, so redeemed / funded value is the diff against the previous
--     balance-bearing event in the same channel.
--
-- A CTE boundary is a sort boundary: ClickHouse sorts the whole 5.2M-row set once PER
-- SCOPE, so having these in two CTEs cost two full sorts. Measured 2026-09-22 on the
-- compiled SQL: two scopes = 2 `Sorting for window` steps, 1.67 GiB peak, 15.6M rows read;
-- one scope = 1 sort, 1.13 GiB, 10.4M rows read, byte-identical output. Giving the first
-- window an explicit ORDER BY is NOT enough -- the specs then match but sit on different
-- table aliases, and it still sorts twice.
--
-- The frames differ and that is fine: one sort feeds both WindowTransforms. w_all's
-- UNBOUNDED/UNBOUNDED frame is the max over the whole partition, i.e. exactly the
-- unordered window it replaces.
windowed AS (
    SELECT
        u.* EXCEPT (source_node_raw, destination_node_raw),
        max(u.source_node_raw)      OVER w_all  AS source_node,
        max(u.destination_node_raw) OVER w_all  AS destination_node,
        anyLast(u.balance_wei)      OVER w_prev AS prev_balance_wei
    FROM unpacked AS u
    WINDOW
        w_all  AS (PARTITION BY u.network, u.channel_id
                   ORDER BY u.block_number, u.log_index
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
        w_prev AS (PARTITION BY u.network, u.channel_id
                   ORDER BY u.block_number, u.log_index
                   ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)

SELECT
    network,
    is_testnet,
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
FROM windowed