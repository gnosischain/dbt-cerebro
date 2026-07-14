{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_holders','granularity:snapshot']
    )
}}

-- Who holds a Circles v2 group's token, resolving BOTH legs:
--   * native ERC-1155 CRC   -> token_address = the group avatar itself
--   * ERC-20 wrapper         -> token_address = a wrapper contract, mapped
--                               back to its group via the wrapper registry
-- avatar_balances_latest keeps the wrapper address as token_address for
-- wrapped rows, so wrapper holders must be joined through int_..._wrappers.
WITH groups AS (
    SELECT DISTINCT lower(avatar) AS group_address
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Group'
),
token_to_group AS (
    SELECT group_address AS token_address, group_address FROM groups
    UNION ALL
    SELECT lower(w.wrapper_address) AS token_address, lower(w.avatar) AS group_address
    FROM {{ ref('int_execution_circles_v2_wrappers') }} w
    WHERE lower(w.avatar) IN (SELECT group_address FROM groups)
),
meta AS (
    SELECT
        avatar,
        argMax(display_name, registered_at) AS display_name
    FROM (
        SELECT
            lower(avatar) AS avatar,
            coalesce(nullIf(metadata_name, ''), nullIf(name, ''), avatar) AS display_name,
            registered_at
        FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
    )
    GROUP BY avatar
)
SELECT
    today() AS as_of_date,
    g.group_address,
    b.avatar AS holder,
    coalesce(m.display_name, b.avatar) AS display_name,
    sum(b.balance) AS balance,
    max(b.is_wrapped) AS is_wrapped
FROM {{ ref('api_execution_circles_v2_avatar_balances_latest') }} b
INNER JOIN token_to_group g ON g.token_address = lower(b.token_address)
LEFT JOIN meta m ON m.avatar = lower(b.avatar)
WHERE b.balance > 0
GROUP BY g.group_address, b.avatar, display_name
