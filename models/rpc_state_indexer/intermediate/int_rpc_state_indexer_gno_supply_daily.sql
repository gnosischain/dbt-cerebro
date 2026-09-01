{{
  config(
    materialized='table',
    order_by='(label, block_date)',
    tags=['production','rpc_state_indexer']
  )
}}

{% set gno_gnosis = "'0x9c58bacc331c9aa871afd802db6379a98e80cedb'" %}
{% set gno_eth    = "'0x6810e776880c02933d47db1b9fc05908e5386b96'" %}
{% set eth_job    = "'daily_gno_supply_wallets'" %}
{% set vesting    = "('0xec83f750adfe0e52a8b0dba6eeb6be5ba0bee535','0x851b7f3ab81bd8df354f0d7640efcd7288553419','0x3257bde8cf067ae6f1ddc0e4b140fe02e3c5e44f')" %}
{% set bridge     = "'0x88ad09518695c6c3712ac10a214be5109a655671'" %}
{% set burned     = "'0x0000000000000000000000000000000000000000'" %}

WITH gnosis_supply AS (
    SELECT
        snapshot_date AS block_date,
        max(toFloat64(scalar_raw)) / 1e18 AS supply
    FROM {{ ref('stg_rpc_state_indexer__token_scalars_published') }}
    WHERE chain_id = 100
      AND token_address = {{ gno_gnosis }}
      AND job_name IN ('daily_token_supply', 'daily_gno_supply_scalar')
      AND scalar_name = 'totalSupply'
    GROUP BY snapshot_date
),

eth_minted AS (
    SELECT
        snapshot_date AS block_date,
        max(toFloat64(scalar_raw)) / 1e18 AS minted
    FROM {{ ref('stg_rpc_state_indexer__token_scalars_published') }}
    WHERE chain_id = 1
      AND token_address = {{ gno_eth }}
      AND job_name = {{ eth_job }}
      AND scalar_name = 'totalSupply'
    GROUP BY snapshot_date
),

wallet_day AS (
    SELECT
        snapshot_date AS block_date,
        holder_address,
        max(toInt256(balance_raw)) AS balance_raw
    FROM {{ ref('stg_rpc_state_indexer__token_balances_published') }}
    WHERE chain_id = 1
      AND token_address = {{ gno_eth }}
      AND job_name = {{ eth_job }}
    GROUP BY block_date, holder_address
),

eth_wallets AS (
    SELECT
        block_date,
        toFloat64(sumIf(balance_raw, holder_address IN {{ vesting }})) / 1e18 AS non_circ,
        toFloat64(sumIf(balance_raw, holder_address = {{ bridge }}))  / 1e18 AS bridge_bal,
        toFloat64(sumIf(balance_raw, holder_address = {{ burned }}))  / 1e18 AS burned_bal
    FROM wallet_day
    GROUP BY block_date
)

SELECT 'Gnosis Circ. Supply' AS label, block_date, supply
FROM (
    SELECT block_date, supply
    FROM gnosis_supply
    ORDER BY block_date WITH FILL STEP 1 INTERPOLATE (supply AS supply)
)

UNION ALL

SELECT 'Non-Circ. Supply' AS label, block_date, supply
FROM (
    SELECT block_date, non_circ AS supply
    FROM eth_wallets
    ORDER BY block_date WITH FILL STEP 1 INTERPOLATE (supply AS supply)
)

UNION ALL

SELECT 'Ethereum Circ. Supply' AS label, block_date, supply
FROM (
    SELECT
        w.block_date,
        m.minted - w.burned_bal - w.non_circ - w.bridge_bal AS supply
    FROM eth_wallets w
    INNER JOIN eth_minted m ON m.block_date = w.block_date
    ORDER BY block_date WITH FILL STEP 1 INTERPOLATE (supply AS supply)
)
