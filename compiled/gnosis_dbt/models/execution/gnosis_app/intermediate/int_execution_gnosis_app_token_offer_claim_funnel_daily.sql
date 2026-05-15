

-- Daily per-offer claim conversion. "Conversion rate" is computed as
--   n_claimers / n_eligible_pool
-- where eligible_pool is the rolling 30-day active GA users on each date.
-- This is an approximation — a true "eligible" set would require knowing
-- which addresses hold the source token at each moment. Documenting the
-- proxy explicitly here so it's not mistaken for a strict eligibility
-- denominator.




WITH active_pool_daily AS (
    -- Rolling 30-day active users as the eligible-pool proxy.
    SELECT
        date,
        uniqExact(address)        AS n_active_30d
    FROM (
        SELECT
            d.date,
            ua.address
        FROM (
            SELECT DISTINCT date
            FROM `dbt`.`int_execution_gnosis_app_user_activity_daily`
            WHERE date < today()
              
                
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily` AS x2
        WHERE 1=1 
      )
    
  

              
        ) d
        INNER JOIN `dbt`.`int_execution_gnosis_app_user_activity_daily` ua
            ON ua.date >  d.date - 30
           AND ua.date <= d.date
           AND ua.activity_kind != 'onboard'
    )
    GROUP BY date
),

claims_daily AS (
    SELECT
        toDate(block_timestamp)                       AS date,
        offer_address                                 AS offer_address,
        count()                                       AS n_claims,
        uniqExact(ga_user)                            AS n_claimers,
        sum(toFloat64OrNull(toString(amount_received_usd))) AS amount_received_usd
    FROM `dbt`.`int_execution_gnosis_app_token_offer_claims`
    WHERE block_timestamp < today()
      
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_token_offer_claim_funnel_daily` AS x2
        WHERE 1=1 
      )
    
  

      
    GROUP BY date, offer_address
)

SELECT
    c.date                                                                AS date,
    c.offer_address                                                       AS offer_address,
    c.n_claims                                                            AS n_claims,
    c.n_claimers                                                          AS n_claimers,
    coalesce(c.amount_received_usd, 0)                                    AS amount_received_usd,
    coalesce(p.n_active_30d, 0)                                           AS n_active_pool_30d,
    round(
        toFloat64(c.n_claimers) / nullIf(toFloat64(p.n_active_30d), 0) * 100,
        2
    )                                                                     AS claim_rate_pct
FROM claims_daily c
LEFT JOIN active_pool_daily p ON p.date = c.date