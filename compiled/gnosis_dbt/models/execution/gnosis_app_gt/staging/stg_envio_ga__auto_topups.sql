

-- Circles auto-topup CONFIG event ledger (grain = opaque event id) — append-only
-- labels {AutoTopupActivated, AutoTopupDeactivated, NewRecipient, ThresholdUpdated}.
-- NOT a Gnosis Pay top-up source. threshold is native BE Int256 -> atoms.
SELECT
    id,
    label,
    lower(owner)            AS owner,
    lower(recipient)        AS recipient,
    lower(recipient_token)  AS recipient_token,
    lower(coordinator)      AS coordinator,
    toFloat64(threshold)    AS threshold_atoms,
    transaction_hash
FROM (
    
SELECT
    id AS id,
    argMax(label, _synced_block) AS label,
    argMax(owner, _synced_block) AS owner,
    argMax(recipient, _synced_block) AS recipient,
    argMax(recipient_token, _synced_block) AS recipient_token,
    argMax(coordinator, _synced_block) AS coordinator,
    argMax(threshold, _synced_block) AS threshold,
    argMax(transaction_hash, _synced_block) AS transaction_hash
FROM `envio_ga`.`auto_topup`
GROUP BY id
HAVING max(_deleted) = 0

)