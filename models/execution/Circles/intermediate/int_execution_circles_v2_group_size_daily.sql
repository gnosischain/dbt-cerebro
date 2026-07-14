{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by=['date', 'group_address'],
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'groups', 'daily']
    )
}}

-- Per-group member count over time. Members = trustees on the group's
-- outgoing trust list. Built from the HISTORICAL trust intervals in
-- int_..._trust_pair_ranges (which retain revoked edges) so historical
-- group sizes are correct, not back-projected from the current snapshot.
-- Each active [valid_from, valid_to] interval is exploded to its days and
-- distinct trustees are counted per (date, group).
WITH groups AS (
    SELECT DISTINCT lower(avatar) AS group_address
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Group'
),
intervals AS (
    SELECT
        lower(r.truster) AS group_address,
        lower(r.trustee) AS trustee,
        toDate(iv.1) AS d_from,
        least(toDate(iv.2), today()) AS d_to
    FROM {{ ref('int_execution_circles_v2_trust_pair_ranges') }} r
    ARRAY JOIN arrayZip(r.valid_from_agg, r.valid_to_agg) AS iv
    WHERE lower(r.truster) IN (SELECT group_address FROM groups)
),
exploded AS (
    SELECT
        group_address,
        trustee,
        d_from + toIntervalDay(arrayJoin(range(0, toUInt32(d_to - d_from) + 1))) AS date
    FROM intervals
    WHERE d_to >= d_from
)
SELECT
    date,
    group_address,
    count(DISTINCT trustee) AS n_members
FROM exploded
WHERE date <= today()
GROUP BY date, group_address
