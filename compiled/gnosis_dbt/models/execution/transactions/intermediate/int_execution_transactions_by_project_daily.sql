







WITH lbl AS (
  SELECT address, project, sector
  FROM `dbt`.`int_crawlers_data_labels`
),

deduped_transactions AS (
    SELECT
        block_timestamp,
        CONCAT('0x', from_address) AS from_address,
        IF(to_address IS NULL, NULL, CONCAT('0x', to_address)) AS to_address,
        gas_used,
        gas_price
    FROM (
        

SELECT block_timestamp, from_address, to_address, gas_used, gas_price
FROM (
    SELECT
        block_timestamp, from_address, to_address, gas_used, gas_price,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`transactions`
    
    WHERE 
    block_timestamp < today()
    AND from_address IS NOT NULL
    AND success = 1
    
      
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(x1.date))
      FROM `dbt`.`int_execution_transactions_by_project_daily` AS x1
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(x2.date, 'UTC'))
      FROM `dbt`.`int_execution_transactions_by_project_daily` AS x2
    )
  

    

    
)
WHERE _dedup_rn = 1

    )
),

tx_labeled AS (
  SELECT
    toDate(t.block_timestamp)                        AS date,
    coalesce(nullIf(trim(l.project), ''), 'Unknown') AS project,
    lower(t.from_address)                            AS from_address,
    toFloat64(coalesce(t.gas_used, 0))               AS gas_used,
    toFloat64(coalesce(t.gas_price, 0))              AS gas_price
  FROM deduped_transactions t
  ANY LEFT JOIN lbl l ON lower(t.to_address) = l.address
),

agg AS (
  SELECT
    date,
    project,
    count()                                    AS tx_count,
    groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
    sum(gas_used)                              AS gas_used_sum,
    sum(gas_used * gas_price) / 1e18           AS fee_native_sum
  FROM tx_labeled
  GROUP BY date, project
),

proj_sector AS (
  SELECT
    project,
    coalesce(nullIf(trim(sector), ''), 'Unknown') AS sector
  FROM (
    SELECT project, anyHeavy(sector) AS sector
    FROM `dbt`.`int_crawlers_data_labels`
    GROUP BY project
  )
)

SELECT
  a.date                AS date,
  a.project             AS project,
  ps.sector             AS sector,
  a.tx_count            AS tx_count,
  a.ua_bitmap_state     AS ua_bitmap_state,
  a.gas_used_sum        AS gas_used_sum,
  a.fee_native_sum      AS fee_native_sum
FROM agg a
LEFT JOIN proj_sector ps ON ps.project = a.project