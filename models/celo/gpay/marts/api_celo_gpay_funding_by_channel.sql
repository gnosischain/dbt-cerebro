{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_funding_by_channel', 'granularity:latest']
  )
}}

-- Funding relationships by shape channel (not MiniPay identity). One row per
-- funding_channel: how many card Safes and funder relationships sit in that
-- bucket, plus USDT/USDC volume where amount is known. See
-- fct_celo_gpay_card_funding for the channel glossary.
--
-- as_of_date is the latest funding this model has observed, taken as a scalar over the
-- whole table rather than max() per group — a per-group max would report each channel's
-- own last funding date and read as if the channels were measured as of different days.
SELECT
    funding_channel                    AS label,
    uniqExact(safe_address)            AS cards,
    count()                            AS relationships,
    round(sumIf(total_amount, total_amount IS NOT NULL), 2) AS amount,
    (SELECT max(toDate(last_funded_at)) FROM {{ ref('fct_celo_gpay_card_funding') }}) AS as_of_date
FROM {{ ref('fct_celo_gpay_card_funding') }}
GROUP BY funding_channel
ORDER BY relationships DESC
