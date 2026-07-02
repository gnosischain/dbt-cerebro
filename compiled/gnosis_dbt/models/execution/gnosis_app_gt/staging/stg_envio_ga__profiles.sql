

-- Circles profile (name / location) per identity. 36 version-superseding
-- duplicate ids exist (CASH round-1), so envio_latest dedup is correctness-required.
SELECT
    lower(id)                   AS address,
    name                        AS profile_name,
    location                    AS profile_location,
    last_updated_block_number
FROM (
    
SELECT
    id AS id,
    argMax(name, _synced_block) AS name,
    argMax(location, _synced_block) AS location,
    argMax(last_updated_block_number, _synced_block) AS last_updated_block_number
FROM `envio_ga`.`profile`
GROUP BY id
HAVING max(_deleted) = 0

)