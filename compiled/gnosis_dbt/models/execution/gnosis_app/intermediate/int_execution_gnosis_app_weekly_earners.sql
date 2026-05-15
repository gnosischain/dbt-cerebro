

-- Weekly "economic earners" set: addresses that earned >= 1 unit of either
-- form of Circles reward in a given week. Used as the right side of the
-- WAU∩earners intersection that defines Weekly Economically Active Users.
--
-- Two reward streams (matching the Dune circles-v2-kpis dashboard):
--   * gCRC cashback   ≥ 1 gCRC from the Circles cashback wallet
--                     → int_execution_circles_v2_gcrc_cashback_recipients_weekly
--   * Inviter fees    ≥ 1 CRC of inviter fees received that week
--                     → int_execution_circles_v2_inviter_fees (aggregated here)



WITH cashback AS (
    SELECT week, address
    FROM `dbt`.`int_execution_circles_v2_gcrc_cashback_recipients_weekly`
    WHERE week >= toDate('2025-11-12')
),

inviter_fees_weekly AS (
    SELECT
        toStartOfWeek(block_timestamp, 1) AS week,
        inviter                           AS address,
        sum(amount)                       AS amount
    FROM `dbt`.`int_execution_circles_v2_inviter_fees`
    WHERE block_timestamp < today()
      AND block_timestamp >= toDateTime('2025-11-12')
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