{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(f_inclusion_slot, inc_dist_cohort)',
        primary_key='(f_inclusion_slot, inc_dist_cohort)'
    ) 
}}

WITH

total_slots AS (
    SELECT
        f_slot
    FROM
        {{ get_postgres('gnosis_chaind', 't_proposer_duties') }}
    {% if is_incremental() %}
    WHERE f_slot > (SELECT max(f_inclusion_slot) FROM {{ this }})
    {% endif %}
),

proposed_slots AS (
    SELECT
        f_slot
    FROM
        {{ get_postgres('gnosis_chaind', 't_blocks') }}
    {% if is_incremental() %}
    WHERE f_slot > (SELECT max(f_inclusion_slot) FROM {{ this }})
    {% endif %}
),

attestations AS (
    SELECT
        f_inclusion_slot,
        f_slot,
        f_inclusion_index
    FROM
        {{ get_postgres('gnosis_chaind', 't_attestations') }}
    {% if is_incremental() %}
    WHERE f_inclusion_slot > (SELECT max(f_inclusion_slot) FROM {{ this }})
    {% endif %}
),

inclusion_distance AS (
    SELECT
        a.f_inclusion_slot,
        a.f_slot,
        a.f_inclusion_index,
        COUNT(DISTINCT p.f_slot) AS inc_dist_cohort
    FROM
        attestations a
    CROSS JOIN
        proposed_slots p
    WHERE
        p.f_slot > a.f_slot AND p.f_slot <= a.f_inclusion_slot
    GROUP BY 1, 2, 3
)

SELECT
    f_inclusion_slot,
    inc_dist_cohort,
    COUNT(*) AS cnt
FROM
    inclusion_distance
GROUP BY 1, 2