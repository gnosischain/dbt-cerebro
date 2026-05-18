{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='group_avatar',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'groups', 'snapshot']
    )
}}

-- Current size of every Circles v2 group, where "size" = the count of
-- distinct trustees on the group's outgoing trust list (the Circles v2
-- group-membership semantic). Groups with zero current members appear with
-- n_members = 0 so the downstream distribution mart can show a real
-- "0 members" bucket.

WITH group_addrs AS (
    SELECT DISTINCT lower(avatar) AS group_avatar
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Group'
),

members AS (
    SELECT
        lower(t.truster)               AS group_avatar,
        count(DISTINCT lower(t.trustee)) AS n_members
    FROM {{ ref('fct_execution_circles_v2_trust_relations_current') }} t
    INNER JOIN group_addrs g ON g.group_avatar = lower(t.truster)
    GROUP BY 1
)

SELECT
    g.group_avatar             AS group_avatar,
    coalesce(m.n_members, 0)   AS n_members
FROM group_addrs g
LEFT JOIN members m USING (group_avatar)
