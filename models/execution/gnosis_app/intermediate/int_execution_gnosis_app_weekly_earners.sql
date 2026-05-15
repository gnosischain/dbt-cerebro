{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(week, address)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','weau','weekly']
  )
}}

-- Weekly "economic earners" set: addresses that earned >= 1 unit of either
-- form of Circles reward in a given week. Used as the right side of the
-- WAU∩earners intersection that defines Weekly Economically Active Users.
--
-- Two reward streams (matching the Dune circles-v2-kpis dashboard):
--   * gCRC cashback   ≥ 1 gCRC from the Circles cashback wallet
--                     → int_execution_circles_v2_gcrc_cashback_recipients_weekly
--   * Inviter fees    ≥ 1 CRC of inviter fees received that week
--                     → int_execution_circles_v2_inviter_fees (aggregated here)

{% set floor_date = var('gnosis_app_wau_floor_date') %}

WITH cashback AS (
    SELECT week, address
    FROM {{ ref('int_execution_circles_v2_gcrc_cashback_recipients_weekly') }}
    WHERE week >= toDate('{{ floor_date }}')
),

inviter_fees_weekly AS (
    SELECT
        toStartOfWeek(block_timestamp, 1) AS week,
        inviter                           AS address,
        sum(amount)                       AS amount
    FROM {{ ref('int_execution_circles_v2_inviter_fees') }}
    WHERE block_timestamp < today()
      AND block_timestamp >= toDateTime('{{ floor_date }}')
    GROUP BY week, inviter
    HAVING amount >= 1
)

SELECT DISTINCT week, address
FROM (
    SELECT week, address FROM cashback
    UNION ALL
    SELECT week, address FROM inviter_fees_weekly
)
WHERE address != ''
