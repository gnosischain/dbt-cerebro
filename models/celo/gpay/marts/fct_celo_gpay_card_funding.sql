{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, funder, token_address)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','funding']
  )
}}

-- Deterministic funding-relationship per card Safe: which addresses fund it
-- (inbound transfers), per token, plus each funder's fan-out. RAW ADDRESSES,
-- NO LABELS — we do not claim a funder "is" a MiniPay wallet, a ramp, or a CEX
-- (MiniPay wallets are unlabelable EOAs; see project notes). The facts are laid
-- out — who funds each card, how much, when, and how many OTHER cards the same
-- funder funds — and the reader draws the conclusion.
--
-- The fan-out column (funder_n_cards_funded) is the honest, label-free signal
-- that distinguishes a shared onboarding/relayer EOA (funds many cards) from an
-- individual funder (funds one) WITHOUT asserting an identity.
--
-- Grain is (safe_address, funder, token_address) so native amounts are summed
-- only within a single token (summing across tokens would be meaningless).
-- amount is populated only for known-decimals tokens; amount_raw always is.

WITH inbound AS (
    SELECT
        safe_address,
        counterparty   AS funder,
        token_address,
        token_symbol,
        amount_raw,
        amount,
        block_time
    FROM {{ ref('int_celo_gpay_safe_transfers_alltoken') }}
    WHERE direction = 'in'
),

funder_fanout AS (
    SELECT
        funder,
        uniqExact(safe_address) AS funder_n_cards_funded,
        count()                 AS funder_n_funding_transfers
    FROM inbound
    GROUP BY funder
),

per_card_funder_token AS (
    SELECT
        safe_address,
        funder,
        token_address,
        any(token_symbol)               AS token_symbol,
        count()                         AS n_transfers,
        min(block_time)                 AS first_funded_at,
        max(block_time)                 AS last_funded_at,
        sum(toInt256(amount_raw))       AS total_amount_raw,
        sum(amount)                     AS total_amount
    FROM inbound
    GROUP BY safe_address, funder, token_address
)

SELECT
    p.safe_address,
    p.funder,
    p.token_address,
    p.token_symbol,
    p.n_transfers,
    p.first_funded_at,
    p.last_funded_at,
    p.total_amount_raw,
    p.total_amount,
    f.funder_n_cards_funded,
    f.funder_n_funding_transfers
FROM per_card_funder_token p
LEFT JOIN funder_fanout f ON f.funder = p.funder
