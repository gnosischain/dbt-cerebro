








WITH

gpay_wallets AS (
    SELECT address
    FROM `dbt`.`int_execution_gpay_wallets`
),

tokens AS (
    SELECT
        lower(address) AS token_address,
        symbol,
        decimals,
        date_start,
        date_end
    FROM `dbt`.`tokens_whitelist`
),


time_bound AS (
    SELECT
        if(
            max(block_timestamp) > toDateTime(0),
            addMinutes(max(block_timestamp), -15),
            now() - INTERVAL 30 MINUTE
        ) AS ts_start
    FROM `dbt`.`int_live__gpay_activity_raw`
),


raw_logs AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        address,
        topic1,
        topic2,
        data
    FROM `execution_live`.`logs`
    WHERE topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
      AND block_timestamp >= (SELECT ts_start FROM time_bound)
      AND (
          topic2 = '0000000000000000000000004822521e6135cd2599199c83ea35179229a172ee'
          OR topic1 = '0000000000000000000000000000000000000000000000000000000000000000'
      )
      AND topic1 IS NOT NULL
      AND topic2 IS NOT NULL
),

transfers AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        concat('0x', l.transaction_hash) AS transaction_hash,
        l.log_index,
        t.token_address,
        t.symbol,
        t.decimals,
        lower(concat('0x', substring(l.topic1, 25, 40))) AS sender,
        lower(concat('0x', substring(l.topic2, 25, 40))) AS receiver,
        reinterpretAsInt256(reverse(unhex(l.data)))       AS value_raw
    FROM raw_logs l
    INNER JOIN tokens t
        ON lower(concat('0x', l.address)) = t.token_address
        AND l.block_timestamp >= t.date_start
        AND (t.date_end IS NULL OR l.block_timestamp < t.date_end)
    WHERE value_raw > 0
),

classified AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        log_index,
        token_address,
        symbol,
        decimals,
        sender,
        receiver,
        value_raw,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets)
             AND receiver = '0x4822521e6135cd2599199c83ea35179229a172ee'
            THEN 'Payment'

            WHEN receiver IN (SELECT address FROM gpay_wallets)
             AND sender = '0x0000000000000000000000000000000000000000'
             AND symbol IN ('EURe', 'GBPe')
            THEN 'Fiat Top Up'

            ELSE NULL
        END AS action,
        CASE
            WHEN sender IN (SELECT address FROM gpay_wallets)
             AND receiver = '0x4822521e6135cd2599199c83ea35179229a172ee'
            THEN sender
            WHEN receiver IN (SELECT address FROM gpay_wallets)
             AND sender = '0x0000000000000000000000000000000000000000'
             AND symbol IN ('EURe', 'GBPe')
            THEN receiver
            ELSE NULL
        END AS wallet_address
    FROM transfers
),

priced AS (
    SELECT
        c.*,
        c.value_raw / POWER(10, c.decimals) AS amount,
        p.price AS token_price_usd
    FROM classified c
    -- ASOF to latest daily price on/before the event date (same idea as
    -- int_live__dex_trades_raw). Exact-date joins leave "today" unpriced
    -- until the next batch price load.
    ASOF LEFT JOIN (
        SELECT symbol, date, price
        FROM `dbt`.`int_execution_token_prices_daily`
        WHERE date >= today() - 7
        ORDER BY symbol, date
    ) p
        ON  p.symbol                 = c.symbol
        AND toDate(c.block_timestamp) >= p.date
    WHERE c.action IS NOT NULL
      AND c.wallet_address IS NOT NULL
      AND c.transaction_hash IS NOT NULL
      AND c.log_index IS NOT NULL
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    wallet_address,
    action,
    symbol,
    token_address,
    sender,
    receiver,
    value_raw,
    amount,
    amount * nullIf(token_price_usd, 0) AS amount_usd
FROM priced