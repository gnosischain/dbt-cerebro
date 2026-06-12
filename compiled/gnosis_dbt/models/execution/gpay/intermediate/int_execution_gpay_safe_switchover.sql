

-- One row per migrated pair. Cutover applies ONLY to refunded ("lost")
-- pairs: the refund injects replacement funds while the old safe may keep
-- residuals, which are recovery-entitled from first_refund_at onward.
-- Non-exploited pairs have NO cutover - their funds count wherever they
-- physically sit, and the old safe drains naturally when the user moves
-- them (many pairs have a deployed new safe but funds not yet moved).
-- new_safe_deployed_at is informational.
-- CH LEFT JOIN fills '' / zero-date on misses, hence the explicit guards.

WITH pairs AS (
    SELECT
        lower(old_safe_address)                                AS old_safe,
        lower(any(new_safe_address))                           AS new_safe,
        min(toDateTime64(parseDateTimeBestEffort(completedAt), 0, 'UTC')) AS completed_at
    FROM `dbt`.`gp_migrated_safes`
    GROUP BY lower(old_safe_address)
),

deployments AS (
    SELECT lower(address) AS new_safe, min(start_blocktime) AS deployed_at
    FROM `dbt`.`contracts_safe_registry`
    GROUP BY lower(address)
),

refunds AS (
    SELECT new_safe, min(refund_date) AS first_refund_date
    FROM `dbt`.`int_execution_gpay_refunds`
    GROUP BY new_safe
)

SELECT
    p.old_safe                                                        AS old_safe,
    p.new_safe                                                        AS new_safe,
    if(d.deployed_at != toDateTime64(0, 0, 'UTC'), d.deployed_at, p.completed_at)
                                                                      AS new_safe_deployed_at,
    if(r.first_refund_date != toDate(0), r.first_refund_date, NULL)   AS first_refund_at,
    toUInt8(r.first_refund_date != toDate(0))                         AS is_lost
FROM pairs p
LEFT JOIN deployments d ON d.new_safe = p.new_safe
LEFT JOIN refunds r     ON r.new_safe = p.new_safe