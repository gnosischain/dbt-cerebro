

-- Daily wallet activity at (date, wallet, direction, counterparty_class, token) grain.
-- Reads the funder-wallet transfer base, so it is cheap and needs no raw-log scan.
--
-- FEE LEGS ARE EXCLUDED HERE, ON PURPOSE, AND THIS IS THE MODEL DOWNSTREAM SHOULD USE.
-- A CIP-64 fee Transfer to the Celo fee sink is not the wallet doing something; it is
-- the gas receipt of whatever it did. Counting it makes a wallet that made one payment
-- look like it made two, and it is the single largest counterparty by count on the
-- chain. The base model keeps those rows (they are real transfers and dropping them
-- from the base would make it non-reconstructable); this model is where they go away.
-- If you need them back, read int_celo_gpay_funder_wallet_transfers directly.
--
-- "Active" throughout the wallet-side tree therefore means: moved a token that was not
-- a gas fee. That is the definition fct_celo_gpay_cardholder_engagement depends on.

-- Source columns are qualified with `t.` throughout. An output alias SHADOWS the
-- source column of the same name in the same SELECT, so an unqualified
-- `sumIf(amount_usd, amount_usd IS NOT NULL) AS amount_usd` resolves its own arguments
-- to the alias and ClickHouse rejects it as an aggregate inside an aggregate
-- (code 184). Keeping the alias identical to the source name is deliberate — the
-- column means the same thing downstream — so the qualification is what makes it legal.
SELECT
    t.block_date                                          AS date,
    t.wallet_address                                      AS wallet_address,
    t.direction                                           AS direction,
    t.counterparty_class                                  AS counterparty_class,
    t.token_symbol                                        AS token_symbol,
    t.token_class                                         AS token_class,
    count()                                               AS n_transfers,
    uniqExact(t.counterparty)                             AS n_counterparties,
    -- Unpriced legs are excluded from the sum rather than coerced to 0. A wallet that
    -- only moved unwhitelisted tokens gets n_transfers > 0 and amount_usd = 0, which is
    -- "active but unvalued", not "moved nothing" — read n_transfers for activity.
    sumIf(t.amount_usd, t.amount_usd IS NOT NULL)         AS amount_usd,
    countIf(t.amount_usd IS NULL)                         AS n_transfers_unpriced
FROM `dbt`.`int_celo_gpay_funder_wallet_transfers` t
WHERE t.counterparty_class != 'fee_sink'

  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_celo_gpay_funder_wallet_daily` AS x1
        WHERE 1=1 
      )
      
    
  

GROUP BY
    date,
    wallet_address,
    direction,
    counterparty_class,
    token_symbol,
    token_class