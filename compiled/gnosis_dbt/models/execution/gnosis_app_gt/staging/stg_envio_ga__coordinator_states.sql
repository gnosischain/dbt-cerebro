

-- Circles auto-topup coordinator current state (grain = coordinator address).
-- One owner per coordinator (1:1). threshold is native BE Int256 -> atoms.
SELECT
    lower(id)               AS coordinator_address,
    lower(owner)            AS owner,
    lower(recipient)        AS recipient,
    lower(recipient_token)  AS recipient_token,
    is_active,
    toFloat64(threshold)    AS threshold_atoms
FROM (
    
SELECT
    id AS id,
    argMax(owner, _synced_block) AS owner,
    argMax(recipient, _synced_block) AS recipient,
    argMax(recipient_token, _synced_block) AS recipient_token,
    argMax(is_active, _synced_block) AS is_active,
    argMax(threshold, _synced_block) AS threshold
FROM `envio_ga`.`coordinator_state`
GROUP BY id
HAVING max(_deleted) = 0

)