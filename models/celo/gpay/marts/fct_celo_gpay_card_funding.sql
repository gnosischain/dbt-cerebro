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
-- (inbound transfers), per token, plus each funder's fan-out and a label-free
-- funding_channel. RAW ADDRESSES, NO IDENTITY LABELS — we do not claim a funder
-- "is" MiniPay, a ramp, or a CEX. funding_channel is a shape classification
-- (envelope + call pattern + fan-out), not an identity; see schema.yml.
--
-- The fan-out column (funder_n_cards_funded) is the honest signal that separates
-- a shared onboarding/relayer EOA (funds many cards) from an individual funder
-- (funds one). CIP-64 alone is not MiniPay — most of Celo uses it.
--
-- Grain is (safe_address, funder, token_address) so native amounts are summed
-- only within a single token (summing across tokens would be meaningless).
-- amount is populated only for known-decimals tokens; amount_raw always is.
--
-- total_amount_raw sums UNSIGNED. These are inbound ERC-20 amounts, which are
-- uint256 and never negative, so there is nothing for a sign bit to express. The
-- previous toInt256 cast would sign-flip any value above 2^255 and turn an inflow
-- into a large negative, silently corrupting the total — the same latent defect the
-- repo already tracks as EXECUTIONTRANSFERS-C03, with the same remedy (match the
-- unsigned source type). Dormant here: the largest amount_raw on Celo is 3.0e9
-- against a 5.8e76 threshold and zero rows exceed it, but a spoof token controls
-- its own mint amount and two of them already reach these cards.

WITH per_card_funder_token AS (
    SELECT
        safe_address,
        funder,
        token_address,
        any(token_symbol)               AS token_symbol,
        count()                         AS n_transfers,
        min(block_time)                 AS first_funded_at,
        max(block_time)                 AS last_funded_at,
        sum(toUInt256(amount_raw))      AS total_amount_raw,
        sum(amount)                     AS total_amount,
        any(funder_n_cards_funded)      AS funder_n_cards_funded,
        -- Relationship-level channel: one value when every transfer agrees, else mixed.
        if(uniqExact(funding_channel) = 1,
           any(funding_channel),
           'mixed')                     AS funding_channel
    FROM {{ ref('int_celo_gpay_funding_tx_envelopes') }}
    GROUP BY safe_address, funder, token_address
),

funder_transfer_count AS (
    SELECT
        funder,
        count() AS funder_n_funding_transfers
    FROM {{ ref('int_celo_gpay_funding_tx_envelopes') }}
    GROUP BY funder
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
    p.funder_n_cards_funded,
    f.funder_n_funding_transfers,
    p.funding_channel
FROM per_card_funder_token p
LEFT JOIN funder_transfer_count f ON f.funder = p.funder
