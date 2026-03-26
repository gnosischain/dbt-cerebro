{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'trusts']
    )
}}

SELECT
    version,
    truster,
    trustee,
    trust_value,
    trust_limit,
    expiry_time,
    valid_from AS updated_at,
    valid_from,
    valid_to
FROM {{ ref('int_execution_circles_trust_relations') }}
WHERE valid_from <= toDateTime({{ circles_chain_now_ts() }})
  AND (valid_to IS NULL OR valid_to > toDateTime({{ circles_chain_now_ts() }}))
  AND is_active = 1
