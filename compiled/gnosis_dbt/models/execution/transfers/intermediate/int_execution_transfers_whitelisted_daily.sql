






WITH tokens AS (
    SELECT
        lower(address)                       AS token_address,
        decimals,
        symbol,
        date_start,
        date_end
    FROM `dbt`.`tokens_whitelist`
    WHERE symbol != 'WxDAI'
),

deduped_logs AS (
    SELECT
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        data,
        block_timestamp
    FROM (
        

SELECT address, topic1, topic2, data, block_timestamp
FROM (
    SELECT
        address, topic1, topic2, data, block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index, log_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`logs`
    
    WHERE 
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND address != 'e91d153e0b41518a2ce8dd3d7944fa863463a97d' -- exclude WxDAI, handled separately
    AND block_timestamp < today()
    
      
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_transfers_whitelisted_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_transfers_whitelisted_daily` AS x2
      WHERE 1=1 
    )
  

    

    
)
WHERE _dedup_rn = 1

    )
),

raw_whitelisted_transfers AS (
    SELECT
        toDate(l.block_timestamp) AS date,
        t.token_address,
        t.symbol,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS "from",
        lower(concat('0x', substring(l.topic2, 25, 40))) AS "to",
        reinterpretAsInt256(
                reverse(unhex(l.data))
            ) AS value_raw
    FROM deduped_logs AS l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address
        AND l.block_timestamp >= t.date_start
        AND (t.date_end IS NULL OR l.block_timestamp < t.date_end)
    WHERE
        toDate(l.block_timestamp) >= t.date_start
        AND (t.date_end IS NULL OR toDate(l.block_timestamp) < t.date_end)
),

transfers_whitelisted_daily AS (
    SELECT
        date,
        token_address,
        any(symbol)       AS symbol,
        "from",
        "to",
        sum(value_raw) AS amount_raw,
        count() AS transfer_count
    FROM raw_whitelisted_transfers
    GROUP BY
        date, token_address, "from", "to"
),


wxdai_logs AS (
    SELECT
        toDate(block_timestamp) AS date
        ,'0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' AS token_address
        ,'WxDAI' AS symbol
        ,decoded_params
        ,event_name
    FROM `dbt`.`contracts_wxdai_events` 
    WHERE (event_name = 'Withdrawal' OR event_name = 'Transfer' OR event_name = 'Deposit')
        AND block_timestamp < today()
        
          
  
    
    

   AND 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_transfers_whitelisted_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_transfers_whitelisted_daily` AS x2
      WHERE 1=1 
    )
  

        
),

deposits_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,'0x0000000000000000000000000000000000000000' AS "from"
        ,decoded_params['dst'] AS "to"
        ,toInt256(decoded_params['wad']) AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Deposit'
),

withdrawals_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,decoded_params['src'] AS "from"
        ,'0x0000000000000000000000000000000000000000' AS "to"
        ,toInt256(decoded_params['wad'])AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Withdrawal'
),

transfers_wxdai AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,decoded_params['src'] AS "from"
        ,decoded_params['dst']  AS "to"
        ,toInt256(decoded_params['wad'])AS value_raw
    FROM wxdai_logs
    WHERE event_name = 'Transfer'
),

transfers_wxdai_daily AS (
    SELECT
        date
        ,token_address
        ,symbol
        ,"from"
        ,"to"
        ,SUM(value_raw) AS value_raw
        ,count() AS transfer_count
    FROM (
        SELECT * FROM deposits_wxdai
        UNION ALL 
        SELECT * FROM withdrawals_wxdai
        UNION ALL 
        SELECT * FROM transfers_wxdai
    )
    GROUP BY 1, 2, 3, 4, 5
),

transfers_daily AS (
    SELECT * FROM transfers_whitelisted_daily
    UNION ALL
    SELECT * FROM transfers_wxdai_daily
)


SELECT
    date,
    token_address,
    symbol,
    "from",
    "to",
    amount_raw,
    transfer_count
FROM transfers_daily