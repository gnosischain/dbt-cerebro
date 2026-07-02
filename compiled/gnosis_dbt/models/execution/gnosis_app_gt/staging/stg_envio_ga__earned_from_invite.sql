

-- GA-native invite-reward ledger (grain = opaque event id; ~5 duplicate ids
-- exist so dedup is required). amount is native BE Int256 CRC (toFloat64/1e18;
-- the "little-endian blob" note was refuted). This is a DIFFERENT source from
-- the circles_v2 inviter_fee stream — must never be reconciled against it.
SELECT
    id,
    lower(invitee_id)               AS invitee_address,
    lower(inviter_id)               AS inviter_address,
    toFloat64(amount) / 1e18        AS amount_crc
FROM (
    
SELECT
    id AS id,
    argMax(amount, _synced_block) AS amount,
    argMax(invitee_id, _synced_block) AS invitee_id,
    argMax(inviter_id, _synced_block) AS inviter_id
FROM `envio_ga`.`earned_from_invite`
GROUP BY id
HAVING max(_deleted) = 0

)