








WITH tokens AS (
    SELECT
        lower(address)                           AS token_address,
        lower(replaceAll(address, '0x', ''))     AS token_address_raw,
        decimals,
        symbol,
        upper(symbol)                            AS symbol_upper,
        date_start,
        date_end
    FROM `dbt`.`tokens_whitelist`
),

deduped_logs AS (
    SELECT
        block_number,
        transaction_index,
        log_index,
        CONCAT('0x', transaction_hash) AS transaction_hash,
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        data,
        block_timestamp
    FROM (
        

SELECT block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp
FROM (
    SELECT
        block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY block_number, transaction_index, log_index
            ORDER BY insert_version DESC
        ) AS _dedup_rn
    FROM `execution`.`logs`
    
    WHERE 
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_transfers_whitelisted_raw` AS x1
        WHERE 1=1 
      )
      
    
  

    
    

    
)
WHERE _dedup_rn = 1

    )
),

raw_whitelisted_logs AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.transaction_index,
        l.log_index,
        lower(l.transaction_hash) AS transaction_hash,
        t.token_address,
        t.symbol,
        t.symbol_upper,
        t.decimals,
        t.date_start,
        t.date_end,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS "from",
        lower(concat('0x', substring(l.topic2, 25, 40))) AS "to",
        toString(
            reinterpretAsUInt256(
                reverse(unhex(replaceAll(l.data, '0x', '')))
            )
        ) AS value_raw
    FROM deduped_logs AS l
    INNER JOIN tokens t
        ON lower(l.address) = t.token_address
       AND toDate(l.block_timestamp) >= t.date_start
       AND (t.date_end IS NULL OR toDate(l.block_timestamp) < t.date_end)
),

prices AS (
    -- Native price hub (replaces the Dune feed). It already unions backedfi (RWA),
    -- native oracle/DEX prices, WXDAI<-XDAI, aTokens, and USD pegs, so a single read
    -- covers what the prices_rwa + prices_dune CTEs did.
    SELECT
        toDate(date)             AS date,
        upper(symbol)            AS symbol_upper,
        price
    FROM `dbt`.`int_execution_token_prices_daily`
),

enriched AS (
    SELECT
        r.block_number,
        r.block_timestamp,
        r.transaction_index,
        r.log_index,
        r.transaction_hash,
        r."from",
        r."to",
        r.token_address,
        r.symbol,
        r.symbol_upper,
        r.decimals,
        r.value_raw,
        r.date_start,
        r.date_end,
        toFloat64OrZero(r.value_raw) / pow(10, r.decimals) AS amount,
        coalesce(
            p.price,
            case
              when r.symbol_upper IN ('USDC','USDC.E','USDT') then 1.0
              when r.symbol_upper = 'WXDAI'                   then 1.0   
              else null
            end
        ) AS price
    FROM raw_whitelisted_logs r
    LEFT JOIN prices p
      ON p.date = toDate(r.block_timestamp)
     AND p.symbol_upper = r.symbol_upper
)

SELECT
    block_number,
    block_timestamp,
    transaction_index,
    log_index,
    transaction_hash,
    "from",
    "to",
    token_address,
    symbol,
    decimals,
    amount,
    price,
    amount * price AS amount_usd,
    value_raw
FROM enriched