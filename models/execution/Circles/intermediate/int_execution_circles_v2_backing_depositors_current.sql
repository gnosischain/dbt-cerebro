{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='backer',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','circles_v2','backing','snapshot']
  )
}}

-- Current snapshot of depositors: every distinct address that has ever
-- emitted a CirclesBackingInitiated event, with summary stats.
--
-- NB: this is the *depositor* set (transactional). The trust-defined
-- *backer* set (members of the backers group's trust list) is not
-- modelled yet — it requires the backers-group avatar address, which
-- is not centralised in dbt_project.yml. Once known, add a second
-- model `int_execution_circles_v2_backers_current` filtered from
-- int_execution_circles_v2_trust_pair_ranges (truster = <backers group>)
-- and the depositors-vs-backers 2x2 cohort.

SELECT
    backer                                                   AS backer,
    min(block_timestamp)                                     AS first_initiated_at,
    max(block_timestamp)                                     AS last_event_at,
    countIf(lifecycle_stage = 'initiated')                   AS n_initiated,
    countIf(lifecycle_stage = 'completed')                   AS n_completed,
    countIf(lifecycle_stage = 'released')                    AS n_released,
    uniqExact(backing_asset)                                 AS n_distinct_assets
FROM {{ ref('int_execution_circles_v2_backing') }}
WHERE backer IS NOT NULL
GROUP BY backer
