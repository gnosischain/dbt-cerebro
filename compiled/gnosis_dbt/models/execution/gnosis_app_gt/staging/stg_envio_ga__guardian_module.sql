

-- Append-only module events (grain = opaque event id). Current membership is
-- derived downstream by NETTING Enabled/Disabled per (safe_address,
-- module_address) — NEVER a per-id argMax latest-state mirror (DBT-D05).
-- id is an opaque base-36 event key, NOT an address (do not lower()).
SELECT
    id,
    label,
    lower(safe_address)     AS safe_address,
    lower(module_address)   AS module_address,
    block_number,
    toDateTime(timestamp)   AS event_at,
    transaction_hash
FROM (
    
SELECT
    id AS id,
    argMax(label, _synced_block) AS label,
    argMax(safe_address, _synced_block) AS safe_address,
    argMax(module_address, _synced_block) AS module_address,
    argMax(block_number, _synced_block) AS block_number,
    argMax(timestamp, _synced_block) AS timestamp,
    argMax(transaction_hash, _synced_block) AS transaction_hash
FROM `envio_ga`.`guardian_module`
GROUP BY id
HAVING max(_deleted) = 0

)