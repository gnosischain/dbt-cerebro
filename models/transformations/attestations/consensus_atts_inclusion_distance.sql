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

proposed_slots AS (
    SELECT
        f_slot
    FROM
        {{ get_postgres('gnosis_chaind', 't_blocks') }}
    {% if is_incremental() %}
    WHERE f_slot > (SELECT min(f_slot) FROM attestations)
      AND f_slot <= (SELECT max(f_inclusion_slot) FROM attestations)
    {% endif %}
),

slot_ranges AS (
    SELECT
        a.f_inclusion_slot,
        a.f_slot AS attestation_slot,
        a.f_inclusion_index,
        arrayJoin(range(a.f_slot + 1, a.f_inclusion_slot + 1)) AS slot
    FROM attestations a
),

inclusion_distance AS (
    SELECT
        sr.f_inclusion_slot,
        sr.attestation_slot,
        sr.f_inclusion_index,
        countDistinct(ps.f_slot) AS inc_dist_cohort
    FROM slot_ranges sr
    LEFT JOIN proposed_slots ps ON sr.slot = ps.f_slot
    GROUP BY
        sr.f_inclusion_slot,
        sr.attestation_slot,
        sr.f_inclusion_index
)

SELECT
    f_inclusion_slot,
    inc_dist_cohort,
    COUNT(*) AS cnt
FROM
    inclusion_distance
GROUP BY 1, 2