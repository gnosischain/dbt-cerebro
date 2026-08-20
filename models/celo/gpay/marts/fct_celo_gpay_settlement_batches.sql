{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(batch_time, transaction_hash)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','settlement']
  )
}}

-- One row per settlement transaction: how many card charges it swept, how much left the
-- contract, where it went, and what the bridge hop cost in CELO. Built on the decoded
-- authoritative event layer (contracts_celo_gpay_settlement_events), not on transfers.
--
-- THERE IS NO SETTLEMENT LAG AND NO FLOAT, which is why this model exists instead of the
-- lag/float marts that were originally scoped. The sweep is ATOMIC: all 476 settlement
-- transactions contain both the TokenPullSuccess charges and the settlement outflow, and
-- there is not one transaction that does only one or the other. A card is charged and the
-- money leaves the contract in the same transaction, so "time from charge to settlement"
-- is zero by construction and "value in flight" never exists. The only residual is
-- cross-token: 10% of (transaction, token) charge groups pull a token that this
-- particular transaction does not settle, so it waits for the next sweep of that token.
-- Do not build a float model on that; measure it here if it ever matters.
--
-- CHARGED AMOUNTS ARE DELIBERATELY TOKEN-AGNOSTIC. The legacy contract's
-- TokenPullSuccess reports the WRONG token — it labels all 1,752 of its pulls USDC, while
-- the chain shows 345 USDC and 1,407 USDT actually arriving. Counts and amounts are
-- right, only the token field lies, and it lies consistently rather than randomly. Both
-- stablecoins carry 6 decimals so the summed amount is still exact; a per-token split of
-- charges is not, and is therefore not offered. See the invariant
-- `legacy-mislabels-the-pulled-token` in models/celo/contracts/schema.yml and
-- docs/lessons/event-field-can-lie.md.
--
-- SETTLED AMOUNTS ARE PER-TOKEN AND TRUSTWORTHY. The outflow events carry the right
-- token on both contracts, verified against raw ERC-20 Transfer logs: legacy USDT out
-- 58,034,396,265 across 221 transfers and USDC out 17,927,696,109 across 166, both
-- matching the events exactly. That asymmetry — inbound token wrong, outbound token
-- right — is the whole reason charges and settlements are sourced differently here.

WITH events AS (
    SELECT
        transaction_hash,
        block_timestamp,
        contract_address,
        event_name,
        decoded_params
    FROM {{ ref('contracts_celo_gpay_settlement_events') }}
),

-- Charge side. Amounts only, no token dimension, for the reason in the header.
charges AS (
    SELECT
        transaction_hash,
        countIf(event_name = 'TokenPullSuccess')          AS n_charges,
        countIf(event_name = 'TokenPullFailedWithAmount') AS n_failed_charges,
        sum(if(event_name = 'TokenPullSuccess',
               toUInt256(decoded_params['amount']), toUInt256(0)))          AS charged_raw,
        -- A failed pull still tells us what GP tried to take.
        sum(if(event_name = 'TokenPullFailedWithAmount',
               toUInt256(decoded_params['originAmount']), toUInt256(0)))    AS failed_raw
    FROM events
    GROUP BY transaction_hash
),

-- Settlement side, per token so it can be priced honestly.
outflows AS (
    SELECT
        e.transaction_hash                             AS transaction_hash,
        toDate(e.block_timestamp)                      AS batch_date,
        lower(e.decoded_params['token'])               AS token_address,
        e.event_name                                   AS event_name,
        toUInt256(e.decoded_params['amount'])          AS amount_raw,
        lower(e.decoded_params['receiver'])            AS receiver,
        nullIf(e.decoded_params['destinationEid'], '') AS destination_eid
    FROM events e
    WHERE e.event_name IN ('SettlementTransferred', 'SettlementBridged')
),

outflows_priced AS (
    SELECT
        o.transaction_hash                                     AS transaction_hash,
        o.event_name                                           AS event_name,
        o.receiver                                             AS receiver,
        o.destination_eid                                      AS destination_eid,
        o.amount_raw                                           AS amount_raw,
        toFloat64(o.amount_raw) / pow(10, w.decimals)          AS amount,
        toFloat64(o.amount_raw) / pow(10, w.decimals) * p.price AS amount_usd
    FROM outflows o
    LEFT JOIN {{ ref('celo_tokens_whitelist') }} w
           ON lower(w.address) = o.token_address
    LEFT JOIN {{ ref('int_celo_token_prices_daily') }} p
           ON p.symbol = w.symbol AND p.date = o.batch_date
),

settlements AS (
    SELECT
        transaction_hash,
        countIf(event_name = 'SettlementBridged')     AS n_bridged,
        countIf(event_name = 'SettlementTransferred') AS n_transferred,
        sumIf(amount,     event_name = 'SettlementBridged')     AS bridged_amount,
        sumIf(amount,     event_name = 'SettlementTransferred') AS transferred_amount,
        sum(amount)                                             AS settled_amount,
        sum(amount_usd)                                         AS settled_usd,
        -- A transaction can both bridge one token and locally transfer another, to
        -- different receivers, so these are sets rather than scalars.
        arraySort(groupUniqArray(receiver))                      AS receivers,
        arraySort(groupUniqArray(destination_eid))               AS destination_eids
    FROM outflows_priced
    GROUP BY transaction_hash
),

-- Exactly one NativeBridgeFeePaid per SettlementBridged, and never more than one bridge
-- per transaction (452 fees / 452 bridges / 452 transactions), so this cannot fan out.
fees AS (
    SELECT
        transaction_hash,
        sum(toFloat64(decoded_params['nativeFee']) / pow(10, 18)) AS native_fee_celo
    FROM events
    WHERE event_name = 'NativeBridgeFeePaid'
    GROUP BY transaction_hash
),

batches AS (
    SELECT
        transaction_hash,
        min(block_timestamp)   AS batch_time,
        any(contract_address)  AS contract_address
    FROM events
    GROUP BY transaction_hash
)

SELECT
    b.batch_time                                            AS batch_time,
    toDate(b.batch_time)                                    AS batch_date,
    b.transaction_hash                                      AS transaction_hash,
    b.contract_address                                      AS settlement_contract,
    coalesce(s.label, 'unknown')                            AS settlement_label,

    toUInt64(c.n_charges)                                   AS n_charges,
    toUInt64(c.n_failed_charges)                            AS n_failed_charges,
    -- Both settlement stablecoins are 6-decimal, so this is exact despite the legacy
    -- contract's unusable token label.
    toFloat64(c.charged_raw) / pow(10, 6)                   AS charged_amount,
    toFloat64(c.failed_raw)  / pow(10, 6)                   AS failed_amount,

    toUInt64(coalesce(st.n_bridged, 0))                     AS n_bridged,
    toUInt64(coalesce(st.n_transferred, 0))                 AS n_transferred,
    coalesce(st.bridged_amount, 0)                          AS bridged_amount,
    coalesce(st.transferred_amount, 0)                      AS transferred_amount,
    coalesce(st.settled_amount, 0)                          AS settled_amount,
    round(coalesce(st.settled_usd, 0), 2)                   AS settled_usd,
    coalesce(st.receivers, [])                              AS receivers,
    coalesce(st.destination_eids, [])                       AS destination_eids,

    -- Only bridged batches pay a fee; a local transfer costs nothing beyond gas.
    coalesce(f.native_fee_celo, 0)                          AS native_fee_celo,
    round(coalesce(f.native_fee_celo, 0) * coalesce(cp.price, 0), 4) AS native_fee_usd,
    -- What one card charge costs GP to settle. NULL rather than 0 for an empty batch so
    -- it is excluded from averages instead of dragging them down.
    if(c.n_charges > 0,
       round(coalesce(f.native_fee_celo, 0) * coalesce(cp.price, 0) / c.n_charges, 6),
       CAST(NULL AS Nullable(Float64)))                     AS fee_usd_per_charge
FROM batches b
LEFT JOIN charges     c  ON c.transaction_hash  = b.transaction_hash
LEFT JOIN settlements st ON st.transaction_hash = b.transaction_hash
LEFT JOIN fees        f  ON f.transaction_hash  = b.transaction_hash
LEFT JOIN {{ ref('celo_gpay_settlement_contracts') }} s
       ON lower(replaceAll(s.address, '0x', '')) = lower(b.contract_address)
LEFT JOIN {{ ref('int_celo_token_prices_daily') }} cp
       ON cp.symbol = 'CELO' AND cp.date = toDate(b.batch_time)
SETTINGS join_use_nulls = 1
