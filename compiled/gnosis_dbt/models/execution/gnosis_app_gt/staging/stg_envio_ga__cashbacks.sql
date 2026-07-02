

-- Gnosis Pay cashback NFT-mint program (grain = mint id). A SEPARATE family
-- from the gCRC-transfer gpay_cashback_* models (CASH-D01) — never a drop-in.
-- status is an integer enum (1 = minted, 0 = reverted). ~47% of rows have an
-- empty gnosis_pay_address, so downstream GP-Safe joins drop ~half.
SELECT
    id,
    lower(owner)                                                 AS owner,
    if(gnosis_pay_address = '', NULL, lower(gnosis_pay_address)) AS gnosis_pay_address,
    status,
    toDateTime(minted_at)                                        AS minted_at,
    minted_block
FROM (
    
SELECT
    id AS id,
    argMax(owner, _synced_block) AS owner,
    argMax(gnosis_pay_address, _synced_block) AS gnosis_pay_address,
    argMax(status, _synced_block) AS status,
    argMax(minted_at, _synced_block) AS minted_at,
    argMax(minted_block, _synced_block) AS minted_block
FROM `envio_ga`.`cashback`
GROUP BY id
HAVING max(_deleted) = 0

)