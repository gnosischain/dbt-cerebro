



-- Per-day, per-conversion-kind tracked-coverage stats. Lets the MTA
-- persona print the coverage block instantly without a self-join.
-- Definition of "tracked": the conversion has at least one touchpoint
-- in the unified events table within a 30-day lookback before
-- conversion_ts, with event_kind != the conversion's mapped event_kind
-- (leakage guard).

WITH conv AS (
    SELECT *
    FROM `dbt`.`int_execution_gnosis_app_conversions`
    WHERE conversion_date < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(conversion_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_coverage_daily` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

events AS (
    SELECT user_pseudonym, event_ts, event_kind
    FROM `dbt`.`int_execution_gnosis_app_user_events_unified`
    -- Pull a wider window to cover every conversion's lookback. Each
    -- conversion's filter happens in the join below.
    WHERE event_date >= (SELECT min(conversion_date) FROM conv) - INTERVAL 30 DAY
      AND event_date < today()
),

tracked AS (
    SELECT
        c.conversion_date,
        c.conversion_kind,
        countDistinct(c.user_pseudonym, c.conversion_ts) AS tracked_conversions,
        uniqExact(c.user_pseudonym)                      AS tracked_users
    FROM conv c
    INNER JOIN events e
      ON  e.user_pseudonym = c.user_pseudonym
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

    GROUP BY c.conversion_date, c.conversion_kind
),

total AS (
    SELECT
        conversion_date,
        conversion_kind,
        count()                  AS total_conversions,
        uniqExact(user_pseudonym) AS total_users
    FROM conv
    GROUP BY conversion_date, conversion_kind
)

SELECT
    t.conversion_date,
    t.conversion_kind,
    t.total_conversions,
    coalesce(tr.tracked_conversions, 0) AS tracked_conversions,
    t.total_users,
    coalesce(tr.tracked_users, 0)       AS tracked_users,
    coalesce(tr.tracked_conversions, 0) / nullIf(t.total_conversions, 0) AS tracked_conversion_coverage,
    coalesce(tr.tracked_users, 0)       / nullIf(t.total_users, 0)       AS tracked_user_coverage
FROM total t
LEFT JOIN tracked tr USING (conversion_date, conversion_kind)