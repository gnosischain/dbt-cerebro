

-- Metri auto-invest accounts (grain = account address). account_address is a
-- different address space from owner (join on owner, never account_address).
-- A synthetic tombstone (id='zz_fake_del_test', _deleted=1 at block 0) is
-- dropped by the envio_latest max(_deleted)=0 guard.
SELECT
    lower(id)                   AS account_address,
    lower(owner)                AS owner,
    lower(coordinator)          AS coordinator,
    lower(investment_token)     AS investment_token,
    is_active
FROM (
    
SELECT
    id AS id,
    argMax(owner, _synced_block) AS owner,
    argMax(coordinator, _synced_block) AS coordinator,
    argMax(investment_token, _synced_block) AS investment_token,
    argMax(is_active, _synced_block) AS is_active
FROM `envio_ga`.`investment_account`
GROUP BY id
HAVING max(_deleted) = 0

)