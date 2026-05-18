{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='date',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'groups', 'supply', 'daily']
    )
}}

-- Daily aggregate of Circles v2 group-token supply, split between native
-- ERC-1155 personal tokens and their ERC-20 wrappers. "Group token" here
-- means a personal CRC token whose issuer avatar has avatar_type = 'Group'.
--
-- Resolution rules for token_address → issuer:
--   * ERC-1155 native: token_address equals the issuer avatar address
--     (Circles v2 packs uint160(avatar) into the token id).
--   * ERC-20 wrapper: token_address is looked up in
--     int_execution_circles_v2_wrappers, where each row maps a wrapper
--     contract to the avatar whose token it wraps.
-- We tag each row with is_wrapped and the resolved issuer, then filter to
-- issuers that are Groups.

WITH group_avatars AS (
    SELECT DISTINCT lower(avatar) AS issuer
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Group'
),

wrappers AS (
    SELECT DISTINCT
        lower(wrapper_address) AS token_address,
        lower(avatar)          AS issuer
    FROM {{ ref('int_execution_circles_v2_wrappers') }}
),

resolved AS (
    SELECT
        b.date          AS date,
        b.token_address AS token_address,
        b.balance       AS balance,
        b.balance_demurraged AS balance_demurraged,
        coalesce(w.issuer, b.token_address) AS issuer,
        w.issuer IS NOT NULL                AS is_wrapped
    FROM {{ ref('fct_execution_circles_v2_avatar_balances_daily') }} b
    LEFT JOIN wrappers w ON w.token_address = b.token_address
),

group_balances AS (
    SELECT r.*
    FROM resolved r
    INNER JOIN group_avatars g ON g.issuer = r.issuer
)

SELECT
    date                                                                    AS date,
    sum(balance)                                                            AS supply_total,
    sumIf(balance, is_wrapped = false)                                      AS supply_native_erc1155,
    sumIf(balance, is_wrapped = true)                                       AS supply_wrapped_erc20,
    sum(coalesce(balance_demurraged, balance))                              AS supply_total_demurraged,
    sumIf(coalesce(balance_demurraged, balance), is_wrapped = false)        AS supply_native_demurraged,
    sumIf(coalesce(balance_demurraged, balance), is_wrapped = true)         AS supply_wrapped_demurraged
FROM group_balances
GROUP BY date
ORDER BY date
