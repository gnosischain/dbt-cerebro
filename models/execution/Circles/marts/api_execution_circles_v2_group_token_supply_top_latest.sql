{{
    config(
        materialized='view',
        tags=['production', 'execution', 'tier1', 'api:circles_v2_group_token_supply_top', 'granularity:latest']
    )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_circles_v2_avatars') }}) AS as_of_date
FROM (
-- Top-N Circles v2 groups by their personal-token supply, with profile
-- columns ready for the dashboard leaderboard table. Member count is
-- joined from fct_execution_circles_v2_group_size_current so the table
-- can show "supply + size" side-by-side.

SELECT
    row_number() OVER (ORDER BY s.supply DESC) AS rank,
    s.group_avatar                            AS group_avatar,
    s.display_name                            AS display_name,
    s.preview_image_url                       AS preview_image_url,
    s.supply                                  AS supply,
    s.wrapped                                  AS wrapped,
    s.unwrapped                                AS unwrapped,
    s.wrapped_pct                              AS wrapped_pct,
    s.supply_demurraged                        AS supply_demurraged,
    coalesce(sz.n_members, toUInt64(0))        AS n_members
FROM {{ ref('fct_execution_circles_v2_group_token_supply_current') }} s
LEFT JOIN {{ ref('fct_execution_circles_v2_group_size_current') }} sz
    ON sz.group_avatar = s.group_avatar
WHERE s.supply > 0
ORDER BY s.supply DESC
LIMIT 100
) AS sub
