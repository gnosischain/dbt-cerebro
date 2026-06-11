{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='old_safe',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gpay']
  )
}}

-- One row per migrated pair with the time from which the NEW safe is the
-- user's wallet and the OLD safe must no longer be counted:
--   refunded ("lost") pairs  -> switch_at = first refund from the verified
--                               distributor (refunds landed after deployment)
--   non-refunded pairs       -> switch_at = new safe deployment (nothing was
--                               lost; the pair simply migrated)
-- CH LEFT JOIN fills '' / zero-date on misses, hence the explicit guards.

WITH pairs AS (
    SELECT
        lower(old_safe_address)                                AS old_safe,
        lower(any(new_safe_address))                           AS new_safe,
        min(toDateTime64(parseDateTimeBestEffort(completedAt), 0, 'UTC')) AS completed_at
    FROM {{ ref('gp_migrated_safes') }}
    GROUP BY lower(old_safe_address)
),

deployments AS (
    SELECT lower(address) AS new_safe, min(start_blocktime) AS deployed_at
    FROM {{ ref('contracts_safe_registry') }}
    GROUP BY lower(address)
),

refunds AS (
    SELECT new_safe, min(refund_date) AS first_refund_date
    FROM {{ ref('int_execution_gpay_refunds') }}
    GROUP BY new_safe
)

SELECT
    p.old_safe                                                        AS old_safe,
    p.new_safe                                                        AS new_safe,
    if(d.deployed_at != toDateTime64(0, 0, 'UTC'), d.deployed_at, p.completed_at)
                                                                      AS new_safe_deployed_at,
    if(r.first_refund_date != toDate(0), r.first_refund_date, NULL)   AS first_refund_at,
    toUInt8(r.first_refund_date != toDate(0))                         AS is_lost,
    if(r.first_refund_date != toDate(0),
       r.first_refund_date,
       toDate(if(d.deployed_at != toDateTime64(0, 0, 'UTC'), d.deployed_at, p.completed_at)))
                                                                      AS switch_at
FROM pairs p
LEFT JOIN deployments d ON d.new_safe = p.new_safe
LEFT JOIN refunds r     ON r.new_safe = p.new_safe
