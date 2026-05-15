-- Pre-joined GP-side journey spine, 60-day lookback. Sensitivity-sweep
-- variant of the canonical 30-day mart. Memory-risky: at 3-role
-- identity fan-out × 60-day lookback × 2-year history this can OOM
-- the 10 GiB cluster cap even at monthly batches; the schema config
-- pins batch_months=1 so refresh.py can retry the rare batch that
-- pushes the limit.










SELECT
    c.user_pseudonym,
    c.identity_role,
    c.conversion_kind,
    c.conversion_ts,
    c.conversion_date,
    c.conversion_amount_usd,
    c.conversion_token,
    e.event_ts                                              AS touch_ts,
    e.event_source,
    e.event_kind,
    e.event_subkind,
    e.event_dedup_key,
    dateDiff('second', e.event_ts, c.conversion_ts)         AS lag_seconds,
    dateDiff('day',    toDate(e.event_ts), c.conversion_date) AS lag_days
FROM `dbt`.`int_execution_gpay_conversions` c
INNER JOIN `dbt`.`int_execution_gpay_user_events_unified` e
    ON  e.user_pseudonym = c.user_pseudonym
    AND e.identity_role  = c.identity_role
    AND e.event_ts       <  c.conversion_ts
    AND e.event_ts       >= c.conversion_ts - INTERVAL 60 DAY
    AND e.event_kind     != 
  multiIf(
    c.conversion_kind = 'topup',                 'chain.topup',
    c.conversion_kind = 'swap_filled',           'chain.swap_filled',
    c.conversion_kind = 'token_offer_claim',     'chain.token_offer_claim',
    c.conversion_kind = 'marketplace_buy',       'chain.marketplace_buy',
    c.conversion_kind = 'gpay_payment',          'gp.payment',
    c.conversion_kind = 'gpay_funded',           'gp.deposit',
    c.conversion_kind = 'gpay_cashback_claim',   'gp.cashback_claim',
    'unknown'
  )

WHERE 1=1
  
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(c.conversion_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`fct_execution_gpay_journeys_60d` AS x1
        WHERE 1=1 
      )
      AND toDate(c.conversion_date) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`fct_execution_gpay_journeys_60d` AS x2
        WHERE 1=1 
      )
    
  

