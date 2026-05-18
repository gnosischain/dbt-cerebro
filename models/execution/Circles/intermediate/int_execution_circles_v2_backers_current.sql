{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='backer',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'backing', 'snapshot']
    )
}}

-- Trust-defined backers snapshot.
--
-- A "backer" is an address currently trusted by the backers group avatar
-- (centralised as var('circles_target_group_address')). This is the
-- canonical population the Circles team refers to as backers — distinct
-- from the depositor set in int_execution_circles_v2_backing_depositors_current,
-- which only counts addresses that emitted a CirclesBackingInitiated event.
-- Not every depositor ends up trusted by the backers group, and the group
-- can trust addresses that never deposited.
--
-- first_trusted_at is the earliest valid_from across all historical trust
-- intervals between the backers group and this trustee. Used downstream
-- by fct_execution_circles_v2_backers_cumulative_daily.

WITH ranges AS (
    SELECT
        lower(trustee) AS backer,
        valid_from_agg
    FROM {{ ref('int_execution_circles_v2_trust_pair_ranges') }}
    WHERE lower(truster) = lower('{{ var("circles_target_group_address") }}')
),

flattened AS (
    SELECT
        backer,
        valid_from
    FROM ranges
    ARRAY JOIN valid_from_agg AS valid_from
)

SELECT
    backer                  AS backer,
    min(valid_from)         AS first_trusted_at
FROM flattened
GROUP BY backer
