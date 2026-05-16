



-- Per-day, per-(conversion_kind, identity_role) coverage stats. Same
-- shape as the GA-side coverage_daily, with the role dimension added so
-- the persona can compare "owner-grain coverage" vs "treasury-grain
-- coverage" for the same conversion_kind.

WITH conv AS (
    SELECT *
    FROM `dbt`.`int_execution_gpay_conversions`
    WHERE conversion_date < today()
      AND identity_role != 'delegate'
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(conversion_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gpay_coverage_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(conversion_date) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`int_execution_gpay_coverage_daily` AS x2
        WHERE 1=1 
      )
    
  

    
),

active_users AS (
    SELECT DISTINCT user_pseudonym, identity_role FROM conv
),

events AS (
    SELECT e.user_pseudonym, e.identity_role, e.event_ts, e.event_kind
    FROM `dbt`.`int_execution_gpay_user_events_unified` e
    INNER JOIN active_users u
        ON e.user_pseudonym = u.user_pseudonym
        AND e.identity_role = u.identity_role
    WHERE e.event_date >= (SELECT min(conversion_date) FROM conv) - INTERVAL 30 DAY
      AND e.event_date <= (SELECT max(conversion_date) FROM conv)
),

tracked AS (
    SELECT
        c.conversion_date,
        c.conversion_kind,
        c.identity_role,
        countDistinct(c.user_pseudonym, c.conversion_ts) AS tracked_conversions,
        uniqExact(c.user_pseudonym)                      AS tracked_users
    FROM conv c
    INNER JOIN events e
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

    GROUP BY c.conversion_date, c.conversion_kind, c.identity_role
),

total AS (
    SELECT
        conversion_date,
        conversion_kind,
        identity_role,
        count()                  AS total_conversions,
        uniqExact(user_pseudonym) AS total_users
    FROM conv
    GROUP BY conversion_date, conversion_kind, identity_role
)

SELECT
    t.conversion_date,
    t.conversion_kind,
    t.identity_role,
    t.total_conversions,
    coalesce(tr.tracked_conversions, 0) AS tracked_conversions,
    t.total_users,
    coalesce(tr.tracked_users, 0)       AS tracked_users,
    coalesce(tr.tracked_conversions, 0) / nullIf(t.total_conversions, 0) AS tracked_conversion_coverage,
    coalesce(tr.tracked_users, 0)       / nullIf(t.total_users, 0)       AS tracked_user_coverage
FROM total t
LEFT JOIN tracked tr USING (conversion_date, conversion_kind, identity_role)