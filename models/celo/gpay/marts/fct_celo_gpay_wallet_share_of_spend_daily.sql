{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, destination)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','celo','gpay','funder_wallet']
  )
}}

{% set large_transfer_usd = 1000 %}

-- Daily split of what cardholders send OUT of their own wallet: how much went to the
-- GP card, and how much went somewhere else. The competitive question, on-chain.
--
-- SOLO FUNDERS ONLY. A hub wallet's outflow is GP's own onboarding, not a user
-- choosing between the card and an alternative, and including it would let ops
-- activity move a behavioural metric.
--
-- STABLECOINS ONLY AND PRICED ONLY. token_class comes from the whitelist seed rather
-- than a symbol list, so a newly-transacting stablecoin joins automatically instead of
-- being silently dropped (models/celo/AGENTS.md). Unpriced legs are excluded from the
-- value columns but still counted in n_transfers.
--
-- THE OUTLIER SPLIT IS THE WHOLE POINT, DO NOT DROP IT. Over 2026-08-05..08 the raw
-- comparison was $365k sent elsewhere against $48.9k to the card, which reads as the
-- card capturing 12% of its holders' money. It does not: SIX transfers of $1,000+
-- carried $313.7k of that $365k (the largest single one was $154k — treasury-scale
-- movement by a handful of addresses, not retail behaviour). Excluding them the two
-- sides are ~$51.4k against ~$48.9k, i.e. roughly half. Both columns are published so
-- the headline cannot be quoted without the correction being one column away.
-- usd_excl_large is the retail-behaviour number; usd_total is the flow-of-funds number.
-- They answer different questions and neither is wrong — mixing them is.
--
-- The median is the honest central measure here: 2026-08-05..08 medians were $20.00 to
-- the card against $1.35 elsewhere. The card is not losing a volume war, it is holding
-- the large ticket while the wallet keeps the micro-payments. Complement, not
-- cannibalisation — but re-check this before repeating it, it is one window.
--
-- Small output; rebuilt whole each run off the wallet transfer base, so it self-heals
-- after any backfill of that base rather than needing a windowed repair.

WITH solo_wallets AS (
    SELECT wallet_address
    FROM {{ ref('int_celo_gpay_funder_wallets') }}
    WHERE is_solo_funder = 1
),

outflow AS (
    SELECT
        t.block_date                                    AS date,
        if(t.counterparty_class = 'gp_card',
           'to_card', 'to_elsewhere')                   AS destination,
        t.wallet_address                                AS wallet_address,
        t.amount_usd                                    AS amount_usd
    FROM {{ ref('int_celo_gpay_funder_wallet_transfers') }} t
    INNER JOIN solo_wallets s ON s.wallet_address = t.wallet_address
    WHERE t.direction = 'out'
      -- Gas receipts are not spending. Already excluded from the daily model; the base
      -- still carries them, so the filter has to be repeated here.
      AND t.counterparty_class != 'fee_sink'
      AND t.token_class = 'STABLECOIN'
      AND t.amount_usd IS NOT NULL
)

SELECT
    date,
    destination,
    count()                                                      AS n_transfers,
    uniqExact(wallet_address)                                    AS n_wallets,
    round(sum(amount_usd), 2)                                    AS usd_total,
    round(sumIf(amount_usd, amount_usd < {{ large_transfer_usd }}), 2)
                                                                 AS usd_excl_large,
    countIf(amount_usd < {{ large_transfer_usd }})               AS n_transfers_excl_large,
    countIf(amount_usd >= {{ large_transfer_usd }})              AS n_transfers_large,
    round(median(amount_usd), 2)                                 AS usd_median
FROM outflow
GROUP BY date, destination
