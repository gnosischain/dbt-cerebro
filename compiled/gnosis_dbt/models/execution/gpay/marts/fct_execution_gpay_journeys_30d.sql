-- Pre-joined GP-side journey spine, 30-day lookback. The 'gpay' sector
-- variant matches identity_role on both conversion + touchpoint sides
-- so role-grain semantics are preserved (treasury vs owner vs delegate).
--
-- Microbatched in 1-month chunks via scripts/full_refresh/refresh.py
-- because the role fan-out (3 roles × all GP activity × 30-day lookback)
-- can transiently OOM at 10 GiB on a single 2-year run. Per-month batches
-- generally fit under the cap; refresh.py auto-retries the rare batch
-- that does push the limit.










WITH conversions AS (
    SELECT *
    FROM `dbt`.`int_execution_gpay_conversions`
    WHERE 1=1
      -- Exclude the delegate identity_role: it represents the shared
      -- gpay delegate wallet (system infrastructure), not a per-user
      -- journey participant. Every gpay user routes through it, so its
      -- 30d event pool averages ~14k touches per (conversion × event_kind)
      -- and dominates the join cost (~99.8% of intermediate rows in
      -- our May-2025 sample). Owner + safe_self preserve the real
      -- end-user journeys.
      AND identity_role != 'delegate'
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(conversion_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`fct_execution_gpay_journeys_30d` AS x1
        WHERE 1=1 
      )
      AND toDate(conversion_date) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`fct_execution_gpay_journeys_30d` AS x2
        WHERE 1=1 
      )
    
  

),
active_users AS (
    SELECT DISTINCT
        user_pseudonym,
        identity_role
    FROM conversions
),
events_window AS (
    SELECT e.*
    FROM `dbt`.`int_execution_gpay_user_events_unified` e
    INNER JOIN active_users u
        ON e.user_pseudonym = u.user_pseudonym
        AND e.identity_role = u.identity_role
    WHERE 1=1
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(e.event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -29))
        FROM `dbt`.`fct_execution_gpay_journeys_30d` AS x1
        WHERE 1=1 
      )
      AND toDate(e.event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.event_date)), -29)
          

        FROM `dbt`.`fct_execution_gpay_journeys_30d` AS x2
        WHERE 1=1 
      )
    
  

),
joined AS (
    SELECT
        c.user_pseudonym                                              AS user_pseudonym,
        c.identity_role                                               AS identity_role,
        c.conversion_kind                                             AS conversion_kind,
        c.conversion_ts                                               AS conversion_ts,
        c.conversion_date                                             AS conversion_date,
        c.conversion_amount_usd                                       AS conversion_amount_usd,
        c.conversion_token                                            AS conversion_token,
        e.event_source                                                AS event_source,
        e.event_kind                                                  AS event_kind,
        e.event_ts                                                    AS touch_ts,
        dateDiff('day', toDate(e.event_ts), c.conversion_date)        AS lag_days
    FROM conversions c
    INNER JOIN events_window e
        ON  e.user_pseudonym = c.user_pseudonym
        AND e.identity_role  = c.identity_role
        AND e.event_ts       <  c.conversion_ts
        AND e.event_ts       >= c.conversion_ts - INTERVAL 30 DAY
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

)
SELECT
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    conversion_date,
    any(conversion_amount_usd)                          AS conversion_amount_usd,
    any(conversion_token)                               AS conversion_token,
    any(event_source)                                   AS event_source,
    event_kind,
    count()                                             AS n_touches,
    min(touch_ts)                                       AS first_touch_ts,
    max(touch_ts)                                       AS last_touch_ts,
    sum(exp(-1.0 * lag_days / 7.0))                     AS td_sum
FROM joined
GROUP BY
    user_pseudonym,
    identity_role,
    conversion_kind,
    conversion_ts,
    conversion_date,
    event_kind
