-- depends_on: `dbt`.`mta_funnels`




-- Daily funnel diagnostics over the unified events table. Each funnel is
-- defined as one row in seed `mta_funnels.csv` (funnel_name + 3 step
-- event_kinds + window_seconds) and materialized into one SELECT block
-- per funnel via the Jinja loop below.
--
-- The loop is required because ClickHouse's `windowFunnel` needs
-- `window_seconds` as a compile-time literal — a CROSS JOIN against the
-- seed with `windowFunnel(f.window_seconds)` is rejected as
-- BAD_ARGUMENTS at runtime.
--
-- To add a new funnel: append a row to mta_funnels.csv AND
-- `dbt seed --select mta_funnels`. The next `dbt run` regenerates this
-- model with the new SELECT block automatically.


  
  


WITH events AS (
  SELECT
    user_pseudonym,
    event_date,
    event_ts,
    event_kind
  FROM `dbt`.`int_execution_gnosis_app_user_events_unified`
  WHERE event_date < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`fct_execution_gnosis_app_funnel_daily` AS x1
        WHERE 1=1 
      )
    
  

  
)








SELECT
  e.event_date                                     AS date,
  'claim_swap_topup'                                     AS funnel_name,
  e.user_pseudonym                                 AS user_pseudonym,
  windowFunnel(2592000)(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = 'chain.token_offer_claim',
    e.event_kind = 'chain.swap_filled'
    
    , e.event_kind = 'chain.topup'
    
  )                                                AS level,
  min(e.event_ts)                                  AS first_event_ts,
  max(e.event_ts)                                  AS last_event_ts
FROM events e
WHERE e.event_kind IN (
  'chain.token_offer_claim',
  'chain.swap_filled'
  
  , 'chain.topup'
  
)
GROUP BY e.event_date, e.user_pseudonym






UNION ALL
SELECT
  e.event_date                                     AS date,
  'modal_swap_topup'                                     AS funnel_name,
  e.user_pseudonym                                 AS user_pseudonym,
  windowFunnel(2592000)(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = 'mp.modal',
    e.event_kind = 'chain.swap_filled'
    
    , e.event_kind = 'chain.topup'
    
  )                                                AS level,
  min(e.event_ts)                                  AS first_event_ts,
  max(e.event_ts)                                  AS last_event_ts
FROM events e
WHERE e.event_kind IN (
  'mp.modal',
  'chain.swap_filled'
  
  , 'chain.topup'
  
)
GROUP BY e.event_date, e.user_pseudonym






UNION ALL
SELECT
  e.event_date                                     AS date,
  'onboard_swap_topup'                                     AS funnel_name,
  e.user_pseudonym                                 AS user_pseudonym,
  windowFunnel(2592000)(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = 'chain.onboard',
    e.event_kind = 'chain.swap_filled'
    
    , e.event_kind = 'chain.topup'
    
  )                                                AS level,
  min(e.event_ts)                                  AS first_event_ts,
  max(e.event_ts)                                  AS last_event_ts
FROM events e
WHERE e.event_kind IN (
  'chain.onboard',
  'chain.swap_filled'
  
  , 'chain.topup'
  
)
GROUP BY e.event_date, e.user_pseudonym






UNION ALL
SELECT
  e.event_date                                     AS date,
  'pageview_swap_topup'                                     AS funnel_name,
  e.user_pseudonym                                 AS user_pseudonym,
  windowFunnel(2592000)(
    toUInt32(toUnixTimestamp(e.event_ts)),
    e.event_kind = 'mp.pageview',
    e.event_kind = 'chain.swap_filled'
    
    , e.event_kind = 'chain.topup'
    
  )                                                AS level,
  min(e.event_ts)                                  AS first_event_ts,
  max(e.event_ts)                                  AS last_event_ts
FROM events e
WHERE e.event_kind IN (
  'mp.pageview',
  'chain.swap_filled'
  
  , 'chain.topup'
  
)
GROUP BY e.event_date, e.user_pseudonym
