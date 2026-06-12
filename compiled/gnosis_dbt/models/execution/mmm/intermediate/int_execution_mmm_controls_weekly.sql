



-- MMM control variables on a continuous weekly spine. Macro / seasonality
-- / operational controls that the MMM analyst joins in alongside KPIs +
-- media. Long-form: one row per (week, control_name).

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

-- GNO + ETH price weekly average (joined later by symbol)
price_feeds AS (
  SELECT
    toStartOfWeek(date, 1)                                              AS week,
    upper(symbol)                                                       AS symbol,
    avg(price)                                                          AS price_avg
  FROM `dbt`.`int_execution_token_prices_daily`
  WHERE upper(symbol) IN ('GNO', 'WETH', 'ETH', 'WXDAI', 'EURE')
  GROUP BY week, symbol
),

ctrl_gno_price AS (
  SELECT
    week,
    'gno_usd_price_avg'                  AS control_name,
    price_avg                            AS control_value,
    'avg'                                AS control_value_method,
    'int_execution_token_prices_daily'   AS source_model
  FROM price_feeds
  WHERE symbol = 'GNO'
),

ctrl_eth_price AS (
  SELECT
    week,
    'eth_usd_price_avg'                  AS control_name,
    price_avg                            AS control_value,
    'avg'                                AS control_value_method,
    'int_execution_token_prices_daily'   AS source_model
  FROM price_feeds
  WHERE symbol IN ('WETH', 'ETH')
),

ctrl_wxdai_price AS (
  SELECT
    week,
    'wxdai_eur_proxy_avg'                AS control_name,
    price_avg                            AS control_value,
    'avg'                                AS control_value_method,
    'int_execution_token_prices_daily'   AS source_model
  FROM price_feeds
  WHERE symbol IN ('WXDAI', 'EURE')
),

-- Chain gas + block count
ctrl_chain_metrics AS (
  SELECT
    toStartOfWeek(date, 1)                                              AS week,
    avg(gas_price_median)                                               AS gas_avg,
    sum(n_txs)                                                          AS block_count
  FROM `dbt`.`int_execution_transactions_info_daily`
  WHERE date < today()
  GROUP BY week
),

ctrl_gas AS (
  SELECT
    week,
    'chain_gas_price_gwei_avg'           AS control_name,
    gas_avg                              AS control_value,
    'avg'                                AS control_value_method,
    'int_execution_transactions_info_daily' AS source_model
  FROM ctrl_chain_metrics
),

ctrl_blocks AS (
  SELECT
    week,
    'chain_block_count'                  AS control_name,
    toFloat64(block_count)               AS control_value,
    'sum'                                AS control_value_method,
    'int_execution_transactions_info_daily' AS source_model
  FROM ctrl_chain_metrics
),

-- Computed: week_of_year, week_index, is_holiday_week, hardfork_step
holidays AS (
  SELECT week, max(is_holiday_week) AS is_holiday_week
  FROM `dbt`.`mmm_holiday_weeks`
  GROUP BY week
),

hardforks AS (
  SELECT
    toStartOfWeek(toDate(fork_date), 1) AS fork_week,
    fork_name
  FROM `dbt`.`mmm_hardfork_steps`
),

computed_rows AS (
  SELECT
    s.week,
    'week_of_year'                       AS control_name,
    toFloat64(toWeek(s.week))            AS control_value,
    'computed'                           AS control_value_method,
    '_derived'                           AS source_model
  FROM spine s

  UNION ALL

  SELECT
    s.week,
    'week_index'                         AS control_name,
    toFloat64(dateDiff('week', (SELECT min(week) FROM spine), s.week)) AS control_value,
    'computed'                           AS control_value_method,
    '_derived'                           AS source_model
  FROM spine s

  UNION ALL

  SELECT
    s.week,
    'is_holiday_week'                    AS control_name,
    toFloat64(coalesce(h.is_holiday_week, toUInt8(0))) AS control_value,
    'computed'                           AS control_value_method,
    'mmm_holiday_weeks'                  AS source_model
  FROM spine s
  LEFT JOIN holidays h ON h.week = s.week

  UNION ALL

  -- hardfork_step = 1 from the fork week onward, 0 before
  SELECT
    s.week,
    'hardfork_step'                      AS control_name,
    toFloat64(if(hf_count.n > 0, 1, 0)) AS control_value,
    'computed'                           AS control_value_method,
    'mmm_hardfork_steps'                 AS source_model
  FROM spine s
  LEFT JOIN (
    SELECT s2.week AS week, count() AS n
    FROM spine s2
    INNER JOIN hardforks hf ON hf.fork_week <= s2.week
    GROUP BY s2.week
  ) hf_count ON hf_count.week = s.week
),

all_feeds AS (
  SELECT week, control_name, control_value, control_value_method, source_model
  FROM ctrl_gno_price
  UNION ALL SELECT week, control_name, control_value, control_value_method, source_model FROM ctrl_eth_price
  UNION ALL SELECT week, control_name, control_value, control_value_method, source_model FROM ctrl_wxdai_price
  UNION ALL SELECT week, control_name, control_value, control_value_method, source_model FROM ctrl_gas
  UNION ALL SELECT week, control_name, control_value, control_value_method, source_model FROM ctrl_blocks
  UNION ALL SELECT week, control_name, control_value, control_value_method, source_model FROM computed_rows
),

registry AS (
  SELECT control_name, control_value_method, source_model
  FROM `dbt`.`mmm_control_registry`
),

filled AS (
  SELECT
    s.week                                                              AS week,
    r.control_name                                                      AS control_name,
    coalesce(f.control_value, CAST(NULL AS Nullable(Float64)))          AS control_value,
    r.control_value_method                                              AS control_value_method,
    r.source_model                                                      AS provenance_model
  FROM spine s
  CROSS JOIN registry r
  LEFT JOIN all_feeds f
    ON f.week = s.week AND f.control_name = r.control_name
)

SELECT * FROM filled
WHERE 1=1

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(week)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.week)), -0))
        FROM `dbt`.`int_execution_mmm_controls_weekly` AS x1
        WHERE 1=1 
      )
    
  

