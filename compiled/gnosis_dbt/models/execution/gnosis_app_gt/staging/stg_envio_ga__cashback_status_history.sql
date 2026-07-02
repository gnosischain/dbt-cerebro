

-- Status transition events for the cashback NFT program (grain = event id).
-- id is an opaque composite key, not an address.
SELECT
    id,
    cashback_id,
    status,
    toDateTime(timestamp)   AS status_at
FROM (
    
SELECT
    id AS id,
    argMax(cashback_id, _synced_block) AS cashback_id,
    argMax(status, _synced_block) AS status,
    argMax(timestamp, _synced_block) AS timestamp
FROM `envio_ga`.`cashback_status_history`
GROUP BY id
HAVING max(_deleted) = 0

)