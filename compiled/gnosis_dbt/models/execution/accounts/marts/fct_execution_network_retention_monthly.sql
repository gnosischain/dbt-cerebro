




-- Per-cohort 30/90/180-day network retention. A "cohort" is the set of
-- addresses whose first-ever successful transaction landed in a given
-- month; retention is the share of that cohort with at least one further
-- active month within 1 / 3 / 6 calendar months after the cohort month.
--
-- Implementation follows the repo's monthly-retention convention (see
-- fct_execution_gpay_cashback_cohort_retention_monthly,
-- fct_execution_gnosis_app_retention_monthly):
--   * the cohort CTE only exposes `cohort_month` (toStartOfMonth(min(...))),
--     never the raw min(first_seen_date) — exposing the latter alongside
--     the wrapped form trips ILLEGAL_AGGREGATION on the new analyzer when
--     used downstream in JOIN/WHERE predicates.
--   * activity is pre-aggregated to one row per (address, active_month);
--     the JOIN is equi-only on address_hash and the time-window filter
--     lives in uniqExactIf.
--
-- The "30 / 90 / 180-day" labels map to month offsets 1 / 3 / 6 (cumulative)
-- — the conventional approximation already used by the other retention
-- marts in this project. Source filters push the cohort_month / activity-
-- date window into the int_ tables so partition pruning trims to the
-- current batch.

WITH cohort AS (
    SELECT
        address_hash,
        toStartOfMonth(min(first_seen_date)) AS cohort_month
    FROM `dbt`.`int_execution_transactions_unique_addresses`
    WHERE 1=1
    
        
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(toStartOfMonth(first_seen_date))) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.cohort_month)), -1))
        FROM `dbt`.`fct_execution_network_retention_monthly` AS x1
        WHERE 1=1 
      )
      AND toDate(toStartOfMonth(first_seen_date)) >= (
        SELECT
          
            addDays(max(toDate(x2.cohort_month)), -1)
          

        FROM `dbt`.`fct_execution_network_retention_monthly` AS x2
        WHERE 1=1 
      )
    
  

    
    GROUP BY address_hash
),

monthly_active AS (
    SELECT
        address_hash,
        toStartOfMonth(date) AS active_month
    FROM `dbt`.`int_execution_transactions_daily_active_addresses`
    WHERE 1=1
    
        AND date >= (
            SELECT toStartOfMonth(addDays(max(toDate(cohort_month)), -1))
            FROM `dbt`.`fct_execution_network_retention_monthly`
        )
    
    GROUP BY address_hash, active_month
)

SELECT
    c.cohort_month                                                                                                                         AS cohort_month,
    toUInt64(uniqExact(c.address_hash))                                                                                                    AS cohort_size,
    toUInt64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 1)))              AS retained_30d,
    toUInt64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 3)))              AS retained_90d,
    toUInt64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 6)))              AS retained_180d,
    toFloat64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 1))) / nullIf(toFloat64(uniqExact(c.address_hash)), 0)  AS retention_rate_30d,
    toFloat64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 3))) / nullIf(toFloat64(uniqExact(c.address_hash)), 0)  AS retention_rate_90d,
    toFloat64(uniqExactIf(c.address_hash, m.active_month > c.cohort_month AND m.active_month <= addMonths(c.cohort_month, 6))) / nullIf(toFloat64(uniqExact(c.address_hash)), 0)  AS retention_rate_180d
FROM cohort c
LEFT JOIN monthly_active m
    ON m.address_hash = c.address_hash
GROUP BY c.cohort_month