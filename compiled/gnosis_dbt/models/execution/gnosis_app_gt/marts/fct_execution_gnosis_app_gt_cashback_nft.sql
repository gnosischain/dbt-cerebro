

-- Gnosis Pay cashback NFT-mint program — daily mint series x status (1=minted,
-- 0=reverted). A SEPARATE family from gpay_cashback_* (gCRC transfers) on a
-- disjoint time axis: NO USD/GNO value column, no shared axis, no pct_delta.
SELECT
    toDate(minted_at)   AS mint_date,
    status,
    count()             AS n_mints,
    uniqExact(owner)    AS n_owners
FROM `dbt`.`stg_envio_ga__cashbacks`
GROUP BY mint_date, status