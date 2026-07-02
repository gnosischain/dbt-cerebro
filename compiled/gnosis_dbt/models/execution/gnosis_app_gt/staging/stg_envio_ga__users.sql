

-- Registered Circles identity universe (~301k). One row per address.
-- lifetime_cashback unit is UNVERIFIED (CASH-D02) — carried as raw atoms only,
-- no /1e18 KPI until decimals are confirmed by the build owner.
SELECT
    lower(id)                     AS address,
    created_at_block,
    toFloat64(lifetime_cashback)  AS lifetime_cashback_atoms
FROM (
    
SELECT
    id AS id,
    argMax(created_at_block, _synced_block) AS created_at_block,
    argMax(lifetime_cashback, _synced_block) AS lifetime_cashback
FROM `envio_ga`.`gnosis_app_user`
GROUP BY id
HAVING max(_deleted) = 0

)