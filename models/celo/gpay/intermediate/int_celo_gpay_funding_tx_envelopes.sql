{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(block_time, tx_hash, log_index)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','funding']
  )
}}

-- One row per inbound transfer onto a GP card Safe, with the funding *transaction's*
-- envelope attached and a label-free funding_channel.
--
-- WHY THIS EXISTS. fct_celo_gpay_card_funding deliberately refuses to label funders as
-- MiniPay / ramp / CEX. That is still right: CIP-64 (transaction_type = 123) is Celo's
-- fee-currency envelope, not a MiniPay ID — ~64% of all Celo txs and ~79% of calls to
-- USDT/USDC use it. What IS discriminative, measured 2026-08-07 against the full funding
-- history, is the COMPOSITE of envelope + call shape + funder fan-out:
--   * CIP-64 + EOA calls token.transfer() + funds exactly one card  → MiniPay-shaped
--   * funder funds 2+ cards                                        → hub / ops path
--   * not a direct EOA→token transfer                              → Safe / router mediated
-- CIP-64-only hubs with ≥3 cards: zero. The 253-card hub 0xa0dd… is type 0, not 123.
--
-- feeCurrency is NOT in celo_execution.transactions (cryo does not store it). RPC
-- samples of cip64_direct_solo first-funds pay gas via the USDT fee adapter
-- 0x0e2a3e05…, matching MiniPay's docs, but that check cannot be done in-warehouse.
--
-- Bounded by inbound transfer count (~6k), so a full rebuild is cheap. The join to
-- transactions is date-bounded to the funding window and restricted to those tx
-- hashes — a bare hash join against the full transactions table OOMs.

WITH inbound AS (
    SELECT
        block_date,
        block_time,
        tx_hash,
        log_index,
        safe_address,
        counterparty AS funder,
        token_address,
        token_symbol,
        amount_raw,
        amount,
        amount_usd
    FROM {{ ref('int_celo_gpay_safe_transfers_alltoken') }}
    WHERE direction = 'in'
),

funder_fanout AS (
    SELECT
        funder,
        uniqExact(safe_address) AS funder_n_cards_funded
    FROM inbound
    GROUP BY funder
),

bounds AS (
    SELECT
        min(block_time) - INTERVAL 1 DAY AS lo,
        max(block_time) + INTERVAL 1 DAY AS hi
    FROM inbound
),

tx AS (
    SELECT
        lower(replaceAll(transaction_hash, '0x', ''))              AS txh,
        any(transaction_type)                                      AS transaction_type,
        any(lower(replaceAll(from_address, '0x', '')))             AS from_addr,
        any(lower(replaceAll(to_address, '0x', '')))               AS to_addr,
        any(substring(lower(replaceAll(input, '0x', '')), 1, 8))   AS sel
    FROM {{ source('celo_execution', 'transactions') }}
    WHERE block_timestamp >= (SELECT lo FROM bounds)
      AND block_timestamp <  (SELECT hi FROM bounds)
      AND lower(replaceAll(transaction_hash, '0x', '')) IN (
            SELECT lower(replaceAll(tx_hash, '0x', '')) FROM inbound
      )
    GROUP BY txh
),

joined AS (
    SELECT
        i.block_date                                              AS block_date,
        i.block_time                                              AS block_time,
        i.tx_hash                                                 AS tx_hash,
        i.log_index                                               AS log_index,
        i.safe_address                                            AS safe_address,
        i.funder                                                  AS funder,
        i.token_address                                           AS token_address,
        i.token_symbol                                            AS token_symbol,
        i.amount_raw                                              AS amount_raw,
        i.amount                                                  AS amount,
        i.amount_usd                                              AS amount_usd,
        tx.transaction_type                                       AS transaction_type,
        -- Direct EOA funding: the funder signed a transfer() on the token contract.
        toUInt8(
            tx.from_addr = lower(replaceAll(i.funder, '0x', ''))
            AND tx.to_addr = lower(replaceAll(i.token_address, '0x', ''))
            AND tx.sel = 'a9059cbb'
        )                                                         AS is_direct_eoa_transfer,
        f.funder_n_cards_funded                                   AS funder_n_cards_funded
    FROM inbound i
    LEFT JOIN tx
           ON tx.txh = lower(replaceAll(i.tx_hash, '0x', ''))
    LEFT JOIN funder_fanout f
           ON f.funder = i.funder
)

SELECT
    block_date,
    block_time,
    tx_hash,
    log_index,
    safe_address,
    funder,
    token_address,
    token_symbol,
    amount_raw,
    amount,
    amount_usd,
    transaction_type,
    is_direct_eoa_transfer,
    funder_n_cards_funded,
    -- Priority: unknown envelope → mediated → hub (fan-out wins over envelope) →
    -- CIP-64 solo → other direct solo. See schema.yml for the channel glossary.
    multiIf(
        transaction_type IS NULL,                          'unknown',
        is_direct_eoa_transfer = 0,                        'mediated',
        funder_n_cards_funded >= 2,                        'hub',
        transaction_type = 123,                            'cip64_direct_solo',
                                                       'other_direct'
    )                                                     AS funding_channel
FROM joined
SETTINGS join_use_nulls = 1
