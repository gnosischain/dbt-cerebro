




-- OpenCover OC-sDAI ERC-4626 vault look-through for the sDAI revenue stream.
--
-- A holder of OC-sDAI shares has indirect sDAI exposure; the vault contract
-- holds the actual sDAI. We value each holder's underlying sDAI in USD so it
-- can feed int_revenue_sdai_fees_daily exactly like native sDAI / Aave aGnosDAI
-- balances:
--   underlying_sDAI = oc_shares * share_price      (share_price = sDAI per share,
--                     from int_yields_ocsdai_share_price_daily, reconstructed
--                     from the vault's ERC-4626 Deposit/Withdraw events)
--   balance_usd     = underlying_sDAI * sDAI_usd_price
--
-- No double count with the native sDAI branch: the vault's own pooled sDAI is
-- held under the vault address, which int_revenue_sdai_fees_daily drops via its
-- non_user_contracts anti-join (the vault enters that list because OC-sDAI is in
-- tokens_whitelist). The vault's self-held OC-sDAI shares (async escrow) are
-- likewise attributed to the vault address here and dropped by the same
-- downstream anti-join.
--
-- INNER JOINs are safe: share_price is forward-filled for every day from the
-- vault's 2026-03-16 launch, and the sDAI USD price is dense; a day cannot be
-- valued without both, so dropping a price-less day is the correct behaviour.

WITH shares AS (
    SELECT
        date,
        address AS user,
        balance AS oc_shares
    FROM `dbt`.`int_execution_tokens_balances_native_daily`
    WHERE symbol = 'OC-sDAI'
      AND date < today()
      AND balance > 0
      AND address IS NOT NULL
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -1))
        FROM `dbt`.`int_revenue_ocsdai_user_balances_daily` AS x1
        WHERE 1=1 
      )
      
    
  

      
),

share_price AS (
    SELECT date, share_price
    FROM `dbt`.`int_yields_ocsdai_share_price_daily`
    WHERE share_price IS NOT NULL
),

sdai_price AS (
    SELECT date, price
    FROM `dbt`.`int_execution_token_prices_daily`
    WHERE symbol = 'sDAI'
      AND price IS NOT NULL
)

SELECT
    s.date AS date,
    s.user AS user,
    round(s.oc_shares * sp.share_price * pr.price, 6) AS balance_usd
FROM shares s
INNER JOIN share_price sp ON sp.date = s.date
INNER JOIN sdai_price  pr ON pr.date = s.date