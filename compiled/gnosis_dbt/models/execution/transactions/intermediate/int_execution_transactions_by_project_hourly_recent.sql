



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
    toStartOfMonth(block_timestamp) >= toStartOfMonth(today() - INTERVAL 1 MONTH)
    AND from_address IS NOT NULL
    AND success = 1

    
)
WHERE _dedup_rn = 1

    )
),

wm AS (
  SELECT toStartOfDay(max(block_timestamp), 'UTC') AS max_day
  FROM deduped_transactions
),

tx AS (
  SELECT
    toStartOfHour(t.block_timestamp, 'UTC') AS hour,
    lower(t.from_address)                 AS from_address,
    lower(t.to_address)                   AS to_address,
    toFloat64(coalesce(t.gas_used, 0))    AS gas_used,
    toFloat64(coalesce(t.gas_price, 0))   AS gas_price
  FROM deduped_transactions t
  CROSS JOIN wm
  WHERE
    toStartOfDay(t.block_timestamp, 'UTC') >= subtractDays(max_day, 2)
    AND toStartOfDay(t.block_timestamp, 'UTC') < max_day
),

classified AS (
  SELECT
    tx.hour,
    coalesce(nullIf(trim(l.project), ''), 'Unknown') AS project,
    count()                                          AS tx_count,
    countDistinct(tx.from_address)                   AS active_accounts,
    groupBitmapState(cityHash64(tx.from_address))    AS ua_bitmap_state,
    sum(tx.gas_used * tx.gas_price) / 1e18           AS fee_native_sum
  FROM tx
  ANY LEFT JOIN lbl l ON tx.to_address = l.address
  GROUP BY tx.hour, project
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
  c.hour,
  c.project,
  ps.sector,
  c.tx_count,
  c.active_accounts,
  c.ua_bitmap_state,
  c.fee_native_sum
FROM classified c
LEFT JOIN proj_sector ps ON ps.project = c.project