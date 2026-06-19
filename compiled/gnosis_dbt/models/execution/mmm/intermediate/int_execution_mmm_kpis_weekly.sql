



-- MMM KPI weekly registry. Long-form: one row per (week, kpi_name).
-- Continuous spine — every (week, kpi_name) within the kpi's active
-- range exists, with kpi_value=0 (or NULL for last-value snapshots) for
-- weeks with no underlying data.
--
-- New KPIs: add a row to seed mmm_kpi_registry.csv AND a corresponding
-- aggregator CTE here. The wide pivot in fct_execution_mmm_spine_weekly
-- picks up the new column automatically.

WITH spine AS (
  
SELECT
  toStartOfWeek(toDate(today() - INTERVAL 730 DAY), 1)
    + toIntervalWeek(n) AS week
FROM (
  SELECT arrayJoin(
    range(0, toUInt32(dateDiff(
      'week',
      toStartOfWeek(toDate(today() - INTERVAL 730 DAY), 1),
      toStartOfWeek(toDate(today() - INTERVAL 7 DAY),   1)
    )) + 1)
  ) AS n
)

),

-- ── KPI source aggregators (each one CTE, weekly grain) ───────────────

kpi_pools_tvl_usd AS (
  SELECT
    toStartOfWeek(date, 1)               AS week,
    'pools_tvl_usd'                      AS kpi_name,
    toFloat64(argMax(tvl_usd, date))     AS kpi_value,
    'last'                               AS kpi_value_method,
    'fct_execution_pools_daily'          AS source_model
  FROM `dbt`.`fct_execution_pools_daily`
  GROUP BY week
),

kpi_pools_volume_usd AS (
  SELECT
    toStartOfWeek(date, 1)               AS week,
    'pools_volume_usd'                   AS kpi_name,
    toFloat64(sum(volume_usd_daily))     AS kpi_value,
    'sum'                                AS kpi_value_method,
    'fct_execution_pools_daily'          AS source_model
  FROM `dbt`.`fct_execution_pools_daily`
  GROUP BY week
),

kpi_dex_volume_usd_dedup AS (
  -- First-hop-only acknowledgment: the upstream fct aggregates each
  -- swap-log row separately, so multi-hop trades are double-counted.
  -- A per-tx dedup CTE on int_execution_pools_dex_trades OOMs at the
  -- 10 GiB cluster cap, so we accept the known multi-hop overcount
  -- here and document it in mmm_kpi_registry.is_dedup_safe = false.
  SELECT
    toStartOfWeek(date, 1)                                                AS week,
    'dex_volume_usd_dedup'                                                AS kpi_name,
    toFloat64(sum(volume_usd))                                            AS kpi_value,
    'sum'                                                                 AS kpi_value_method,
    'fct_execution_trades_by_protocol_daily'                              AS source_model
  FROM `dbt`.`fct_execution_trades_by_protocol_daily`
  GROUP BY week
),

kpi_ga_active_users AS (
  SELECT
    toStartOfWeek(date, 1)               AS week,
    'ga_active_users'                    AS kpi_name,
    toFloat64(argMax(active_users, date)) AS kpi_value,
    'last'                               AS kpi_value_method,
    'fct_execution_gnosis_app_users_daily' AS source_model
  FROM `dbt`.`fct_execution_gnosis_app_users_daily`
  GROUP BY week
),

kpi_ga_new_users AS (
  SELECT
    toStartOfWeek(date, 1)               AS week,
    'ga_new_users'                       AS kpi_name,
    toFloat64(sum(new_users))            AS kpi_value,
    'sum'                                AS kpi_value_method,
    'fct_execution_gnosis_app_users_daily' AS source_model
  FROM `dbt`.`fct_execution_gnosis_app_users_daily`
  GROUP BY week
),

kpi_gpay_topups AS (
  -- Source already at weekly grain
  SELECT
    week,
    'gpay_topups_count'                  AS kpi_name,
    toFloat64(n_topups)                  AS kpi_value,
    'sum'                                AS kpi_value_method,
    'fct_execution_gnosis_app_gpay_topups_weekly' AS source_model
  FROM `dbt`.`fct_execution_gnosis_app_gpay_topups_weekly`
),

kpi_gpay_topups_volume AS (
  SELECT
    week,
    'gpay_topups_volume_usd'             AS kpi_name,
    toFloat64(volume_usd)                AS kpi_value,
    'sum'                                AS kpi_value_method,
    'fct_execution_gnosis_app_gpay_topups_weekly' AS source_model
  FROM `dbt`.`fct_execution_gnosis_app_gpay_topups_weekly`
),

-- Union all per-KPI feeds, then join to the spine + registry to fill
-- missing weeks with 0 / NULL.
all_feeds AS (
  SELECT * FROM kpi_pools_tvl_usd
  UNION ALL SELECT * FROM kpi_pools_volume_usd
  UNION ALL SELECT * FROM kpi_dex_volume_usd_dedup
  UNION ALL SELECT * FROM kpi_ga_active_users
  UNION ALL SELECT * FROM kpi_ga_new_users
  UNION ALL SELECT * FROM kpi_gpay_topups
  UNION ALL SELECT * FROM kpi_gpay_topups_volume
),

registry AS (
  SELECT kpi_name, kpi_value_method, units, source_model, is_dedup_safe
  FROM `dbt`.`mmm_kpi_registry`
),

filled AS (
  SELECT
    s.week                                                              AS week,
    r.kpi_name                                                          AS kpi_name,
    coalesce(
      f.kpi_value,
      if(r.kpi_value_method = 'sum', toFloat64(0), CAST(NULL AS Nullable(Float64)))
    )                                                                   AS kpi_value,
    r.kpi_value_method                                                  AS kpi_value_method,
    r.is_dedup_safe                                                     AS is_dedup_safe,
    r.source_model                                                      AS provenance_model
  FROM spine s
  CROSS JOIN registry r
  LEFT JOIN all_feeds f
    ON f.week = s.week AND f.kpi_name = r.kpi_name
)

SELECT * FROM filled
WHERE 1=1

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(week)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.week)), -0))
        FROM `dbt`.`int_execution_mmm_kpis_weekly` AS x1
        WHERE 1=1 
      )
      
    
  

