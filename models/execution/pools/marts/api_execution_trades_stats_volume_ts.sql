{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'pools', 'trades', 'api']
    )
}}

-- Daily USD volume from DEX trades across Gnosis Chain, stacked by protocol.
-- Volume is summed per swap event (per hop), so multi-hop trades contribute
-- to every protocol they touch. Used by the Trades → Stats tab.

SELECT
    toDate(block_timestamp)          AS date,
    protocol                         AS label,
    round(sum(amount_usd), 0)        AS value
FROM {{ ref('int_execution_pools_dex_trades') }}
WHERE amount_usd IS NOT NULL
  AND protocol != ''
  AND block_timestamp < today()
GROUP BY date, label
ORDER BY date, label
