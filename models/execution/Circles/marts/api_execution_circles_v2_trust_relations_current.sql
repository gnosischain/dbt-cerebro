{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_trust_relations_current', 'granularity:snapshot']
    )
}}

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM {{ ref('int_execution_circles_v2_trust_updates') }}) AS as_of_date
FROM (
SELECT
    truster,
    trustee,
    valid_from,
    valid_to
FROM {{ ref('fct_execution_circles_v2_trust_relations_current') }}
ORDER BY valid_from DESC
) AS sub
