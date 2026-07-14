{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_group_explorer_profile','granularity:latest']
    )
}}

-- One row per Circles v2 group: identity + on-chain handlers + snapshot
-- KPIs. Backs the Group Explorer identity header AND every KPI tile.
-- Handlers use argMaxIf so a value set only in an early settings event is
-- not nulled out by a later event that didn't carry it.
WITH settings AS (
    SELECT
        group_address,
        argMaxIf(owner,              block_timestamp, owner != '')              AS owner,
        argMaxIf(treasury_address,   block_timestamp, treasury_address != '')  AS treasury_address,
        argMaxIf(service,            block_timestamp, service != '')           AS service,
        argMaxIf(mint_handler,       block_timestamp, mint_handler != '')      AS mint_handler,
        argMaxIf(redemption_handler, block_timestamp, redemption_handler != '') AS redemption_handler
    FROM (
        SELECT lower(group_address) AS group_address, owner, treasury_address,
               service, mint_handler, redemption_handler, block_timestamp
        FROM {{ ref('int_execution_circles_v2_group_settings_updates') }}
    )
    GROUP BY group_address
),
supply AS (
    -- group_token_supply_current inherits duplicate rows from avatar_metadata
    -- for a few groups; collapse to one row per group.
    SELECT group_address, max(supply) AS supply, max(wrapped_pct) AS wrapped_pct
    FROM (
        SELECT lower(group_avatar) AS group_address, supply, wrapped_pct
        FROM {{ ref('fct_execution_circles_v2_group_token_supply_current') }}
    )
    GROUP BY group_address
),
size AS (
    SELECT group_address, max(n_members) AS n_members
    FROM (
        SELECT lower(group_avatar) AS group_address, n_members
        FROM {{ ref('fct_execution_circles_v2_group_size_current') }}
    )
    GROUP BY group_address
),
collat AS (
    SELECT group_address, sum(balance_raw) / 1e18 AS collateral_total
    FROM (
        SELECT lower(group_address) AS group_address, balance_raw, date
        FROM {{ ref('int_execution_circles_v2_group_collateral_balances_daily') }}
    )
    WHERE date = (SELECT max(date) FROM {{ ref('int_execution_circles_v2_group_collateral_balances_daily') }})
    GROUP BY group_address
),
mints7d AS (
    SELECT lower(token_address) AS group_address, sum(amount_raw) / 1e18 AS mints_7d
    FROM {{ ref('int_execution_circles_v2_mint_events') }}
    WHERE mint_kind = 'group' AND block_timestamp >= now() - INTERVAL 7 DAY
    GROUP BY lower(token_address)
),
holders AS (
    SELECT group_address, count(DISTINCT holder) AS holders_count
    FROM {{ ref('api_execution_circles_v2_group_holders') }}
    GROUP BY group_address
),
groups_meta AS (
    -- one row per group avatar (avatar_metadata can carry >1 registration row)
    SELECT
        group_address,
        argMax(display_name, registered_at)       AS display_name,
        argMax(preview_image_url, registered_at)  AS preview_image_url,
        max(registered_at)                        AS last_registered_at,
        argMax(invited_by, registered_at)         AS invited_by
    FROM (
        SELECT
            lower(avatar) AS group_address,
            coalesce(nullIf(metadata_name, ''), nullIf(name, ''), avatar) AS display_name,
            metadata_preview_image_url AS preview_image_url,
            registered_at,
            invited_by
        FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
        WHERE avatar_type = 'Group'
    )
    GROUP BY group_address
)
SELECT
    today() AS as_of_date,
    m.group_address AS group_address,
    m.display_name AS display_name,
    m.preview_image_url AS preview_image_url,
    m.last_registered_at AS registered_at,
    m.invited_by AS invited_by,
    s.owner,
    s.treasury_address,
    s.service,
    s.mint_handler,
    s.redemption_handler,
    coalesce(sz.n_members, 0)       AS n_members,
    coalesce(sup.supply, 0)         AS supply,
    coalesce(sup.wrapped_pct, 0)    AS wrapped_pct,
    coalesce(c.collateral_total, 0) AS collateral_total,
    coalesce(mi.mints_7d, 0)        AS mints_7d,
    coalesce(h.holders_count, 0)    AS holders_count
FROM groups_meta m
LEFT JOIN settings s ON s.group_address = m.group_address
LEFT JOIN supply  sup ON sup.group_address = m.group_address
LEFT JOIN size    sz  ON sz.group_address = m.group_address
LEFT JOIN collat  c   ON c.group_address = m.group_address
LEFT JOIN mints7d mi  ON mi.group_address = m.group_address
LEFT JOIN holders h   ON h.group_address = m.group_address
