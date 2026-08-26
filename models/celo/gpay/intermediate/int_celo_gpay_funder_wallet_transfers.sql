{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(wallet_address, block_time, tx_hash, log_index)',
    partition_by='toStartOfMonth(block_date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','transfers','funder_wallet'],
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

{% set wallet_start = '2026-01-01' %}  {# GP era floor, same as the card transfer base #}

{# Celo's CIP-64 fee recipient. Every fee-currency transaction emits a Transfer to it,
   so it is BY FAR the highest-count counterparty of any Celo wallet and it is pure
   noise: 3,799 legs from 557 cardholder wallets over 2026-08-05..08 carrying $10.30
   in total (median $0.0019). Left unclassified it dominates every counterparty
   ranking and inflates every transfer count. It is classified here, once, so no
   downstream model has to remember it. It is also chain-wide, which is exactly why
   CIP-64 is not a MiniPay marker. #}
{% set celo_fee_sink = '000000000000000000000000000000000ce106a5' %}
{% set zero_address  = '0000000000000000000000000000000000000000' %}

-- All-token ERC-20 Transfer activity touching a GP card FUNDING WALLET on either
-- side, with the counterparty classified against the GP universe.
--
-- This is the wallet-side twin of int_celo_gpay_safe_transfers_alltoken: that model
-- sees what the CARD does, this one sees what the cardholder does with the rest of
-- their money. Everything the card-side tree can measure stops at the Safe boundary,
-- and on 2026-08-05..08 that boundary hid roughly ten times more transfer activity
-- than it showed (18,157 wallet legs vs 1,674 card actions for the same people).
--
-- counterparty_class is the point of the model. Four of the five classes are
-- deterministic set membership against models in this tree; 'other' is honestly
-- unknown and must never be presented as "merchants" or "P2P" — we do not know what
-- it is. Note that recurring 'other' recipients receiving from 90+ distinct
-- cardholder wallets at $1-5 medians do exist and emit no logs (they are EOAs), but
-- naming them would be a guess.
--
-- SAME NULL-FOR-UNKNOWN CONTRACT as the card transfer base: amount / amount_usd /
-- token_symbol / token_class are populated only for whitelisted (address-gated)
-- tokens; everything else keeps amount_raw and leaves the rest NULL rather than
-- guessing a decimal scale. That is why join_use_nulls = 1 is set in the pre_hook —
-- at the ClickHouse default the whitelist LEFT JOIN yields decimals = 0 for a
-- non-whitelisted token and `amount` silently becomes the raw integer word
-- (docs/lessons/ch-left-join-nulls.md).
--
-- COST. Scans all Celo Transfer logs in the window and keeps only rows with a funding
-- wallet on one side, so it carries the same hazard as the card base: a single-query
-- full refresh OOMs (ClickHouse code 241). Rebuild through
-- scripts/full_refresh/refresh.py in the monthly batches declared in meta.full_refresh.
-- incremental_strategy resolves to `append` when start_month is set, because a staged
-- window narrower than the month partition would make insert_overwrite's REPLACE
-- PARTITION discard the rest of that month (docs/lessons/staged-insert-overwrite-wipe.md).
-- The scoped-append path fills EMPTY months only: re-running it over a populated month
-- appends a second copy (docs/lessons/append-over-populated-duplicates.md). Drop the
-- partition first (macros/db/drop_partition) to reprocess.
--
-- The floor is the GP era, not the wallet's whole life. Wallet TENURE (how long the
-- holder was active on Celo before their card existed) needs history back to the L2
-- migration and is deliberately NOT modelled here — that is a separate, much wider
-- scan and it must not be bolted onto this model's partitions.

WITH wallets AS (
    SELECT lower(replaceAll(wallet_address, '0x', '')) AS addr
    FROM {{ ref('int_celo_gpay_funder_wallets') }}
),

registry AS (
    SELECT lower(replaceAll(address, '0x', '')) AS addr
    FROM {{ ref('int_celo_gpay_safe_registry') }}
),

whitelist AS (
    SELECT
        lower(replaceAll(address, '0x', '')) AS token_addr,
        symbol,
        decimals,
        token_class
    FROM {{ ref('celo_tokens_whitelist') }}
),

transfer_logs AS (
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY block_number, transaction_index, log_index
                ORDER BY insert_version DESC
            ) AS _dedup_rn
        FROM {{ source('celo_execution', 'logs') }}
        WHERE replaceAll(topic0, '0x', '') = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- Transfer
          AND block_timestamp >= toDateTime('{{ wallet_start }}')
          -- Full-refresh batching: refresh.py passes start_month/end_month per monthly
          -- batch (see meta.full_refresh) so this all-Transfer scan is bounded to one
          -- month at a time and never materialises the whole logs table in one query.
          {% if start_month is not none and end_month is not none %}
          AND block_timestamp >= toDateTime('{{ start_month }}')
          AND block_timestamp <  toDateTime('{{ end_month }}') + INTERVAL 1 MONTH
          {% endif %}
          {{ apply_monthly_incremental_filter('block_timestamp', 'block_date', true) }}
    )
    WHERE _dedup_rn = 1
),

decoded AS (
    SELECT
        toDate(l.block_timestamp)                                     AS block_date,
        l.block_timestamp                                            AS block_time,
        concat('0x', lower(replaceAll(l.transaction_hash, '0x', ''))) AS tx_hash,
        l.log_index                                                  AS log_index,
        substring(replaceAll(l.topic1, '0x', ''), 25, 40)            AS from_raw,
        substring(replaceAll(l.topic2, '0x', ''), 25, 40)            AS to_raw,
        lower(replaceAll(l.address, '0x', ''))                       AS token_addr,
        reinterpretAsUInt256(reverse(unhex(
            substring(replaceAll(l.data, '0x', ''), 1, 64)
        )))                                                          AS amount_raw
    FROM transfer_logs l
    WHERE substring(replaceAll(l.topic1, '0x', ''), 25, 40) IN (SELECT addr FROM wallets)
       OR substring(replaceAll(l.topic2, '0x', ''), 25, 40) IN (SELECT addr FROM wallets)
),

-- One row per (transfer, wallet-side). A wallet-to-wallet transfer between two
-- cardholders legitimately produces TWO rows, one per side's own footprint — the
-- same convention as the card transfer base. Do not dedupe across sides.
outbound AS (
    SELECT
        block_date, block_time, tx_hash, log_index,
        from_raw   AS wallet_raw,
        'out'      AS direction,
        to_raw     AS counterparty_raw,
        token_addr, amount_raw
    FROM decoded
    WHERE from_raw IN (SELECT addr FROM wallets)
),

inbound AS (
    SELECT
        block_date, block_time, tx_hash, log_index,
        to_raw     AS wallet_raw,
        'in'       AS direction,
        from_raw   AS counterparty_raw,
        token_addr, amount_raw
    FROM decoded
    WHERE to_raw IN (SELECT addr FROM wallets)
),

unioned AS (
    SELECT * FROM outbound
    UNION ALL
    SELECT * FROM inbound
)

-- Every projected column carries an explicit alias, including pass-throughs: under the
-- legacy analyzer a bare `u.block_date` is named `u.block_date` in the result header,
-- so partition_by/order_by cannot resolve at CREATE TABLE time (CH code 47).
SELECT
    u.block_date                                                      AS block_date,
    u.block_time                                                      AS block_time,
    u.tx_hash                                                         AS tx_hash,
    u.log_index                                                       AS log_index,
    concat('0x', u.wallet_raw)                                        AS wallet_address,
    u.direction                                                       AS direction,
    concat('0x', u.counterparty_raw)                                  AS counterparty,
    multiIf(
        u.counterparty_raw = '{{ celo_fee_sink }}',                   'fee_sink',
        u.counterparty_raw = '{{ zero_address }}',                    'zero_address',
        u.counterparty_raw IN (SELECT addr FROM registry),            'gp_card',
        u.counterparty_raw IN (SELECT addr FROM wallets),             'cardholder_wallet',
                                                                      'other'
    )                                                                 AS counterparty_class,
    concat('0x', u.token_addr)                                        AS token_address,
    w.symbol                                                          AS token_symbol,
    w.token_class                                                     AS token_class,
    u.amount_raw                                                      AS amount_raw,
    if(w.decimals IS NULL, NULL, toFloat64(u.amount_raw) / pow(10, w.decimals)) AS amount,
    if(w.decimals IS NULL, NULL,
       (toFloat64(u.amount_raw) / pow(10, w.decimals)) * nullIf(p.price, 0))    AS amount_usd
FROM unioned u
LEFT JOIN whitelist w ON u.token_addr = w.token_addr
LEFT JOIN {{ ref('int_celo_token_prices_daily') }} p
    ON p.date = u.block_date AND p.symbol = w.symbol
