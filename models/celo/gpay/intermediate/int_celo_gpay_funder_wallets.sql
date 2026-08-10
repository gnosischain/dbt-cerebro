{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='wallet_address',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','native','funder_wallet']
  )
}}

-- The card <-> funding-wallet identity spine: one row per EOA that has directly
-- funded a GP card Safe.
--
-- WHY THIS EXISTS. On Celo the Safe OWNER is useless as identity: GP's provisioning
-- flow disables owner-based signing, so int_celo_gpay_safe_current_owners returns the
-- dead sentinel 0x..02 for every card, and the funder equals the owner in ZERO cases
-- (verified 2026-08-10 across all 1,015 solo-funded cards). The funding EOA is
-- therefore the ONLY on-chain link between a card and the human behind it, and
-- everything we can learn about cardholder behaviour off the card hangs off this
-- table.
--
-- SCOPE = DIRECT EOA FUNDERS ONLY (is_direct_eoa_transfer = 1). A 'mediated' funder is
-- a Safe or router contract that moved money on someone's behalf; it is not a person's
-- wallet and following its transfer history would pool unrelated users together.
-- Hubs (funders of 2+ cards) ARE kept, because dropping them silently would hide
-- GP's own onboarding paths — but they are flagged, never blended with user wallets.
-- Filter on is_solo_funder for any user-behaviour metric.
--
-- NOT AN IDENTITY LABEL. This says "the wallet that funded this card". It does NOT
-- say MiniPay: there is no MiniPay marker on Celo, and CIP-64 is a chain-wide fee
-- envelope, not an app tag (models/celo/AGENTS.md). The composite shape recorded in
-- funding_channel is as close as the chain gets, and it stays a shape, not a name.
--
-- Bounded by the funding-relationship count (~6k rows in), so a full rebuild is cheap.

WITH direct_funding AS (
    SELECT
        funder,
        safe_address,
        block_time,
        funding_channel,
        amount_usd
    FROM {{ ref('int_celo_gpay_funding_tx_envelopes') }}
    -- Contract-mediated and unknown-envelope funders are excluded here, not flagged:
    -- they are not wallets, so a per-wallet history over them would be meaningless.
    WHERE is_direct_eoa_transfer = 1
)

SELECT
    funder                                                    AS wallet_address,
    uniqExact(safe_address)                                   AS n_cards_funded,
    -- The 1:1 case is the analytically useful one: exactly one card, so the wallet's
    -- off-card behaviour can be attributed to that cardholder without ambiguity.
    -- 1,015 of the funders on 2026-08-10; the rest are onboarding/ops fan-out.
    toUInt8(uniqExact(safe_address) = 1)                      AS is_solo_funder,
    -- Only meaningful for solo funders. For a hub this would be an arbitrary pick of
    -- one of many cards, so it is deliberately NULL rather than misleading.
    if(uniqExact(safe_address) = 1,
       argMin(safe_address, block_time),
       CAST(NULL AS Nullable(String)))                        AS card_safe_address,
    count()                                                   AS n_funding_transfers,
    min(block_time)                                           AS first_funded_at,
    max(block_time)                                           AS last_funded_at,
    argMin(funding_channel, block_time)                       AS first_funding_channel,
    -- Whitelisted tokens only carry amount_usd; unpriced legs stay out of the sum
    -- rather than being coerced to 0 (a $0 funder would read as a real observation).
    sumIf(amount_usd, amount_usd IS NOT NULL)                 AS total_funded_usd
FROM direct_funding
GROUP BY wallet_address
