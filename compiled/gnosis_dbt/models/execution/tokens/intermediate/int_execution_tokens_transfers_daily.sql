




WITH base AS (
    SELECT
        date,
        lower(token_address) AS token_address,
        symbol,
        lower("from")        AS from_address,
        lower("to")          AS to_address,
        amount               AS amount,
        amount_usd           AS amount_usd,
        transfer_count       AS transfer_count
    FROM `dbt`.`int_execution_transfers_whitelisted_daily`
    WHERE date < today()
      
        
  

      
),

with_class AS (
    SELECT
        b.date,
        b.token_address,
        b.symbol,
        coalesce(w.token_class, 'OTHER') AS token_class,
        b.amount,
        b.amount_usd,
        b.from_address,
        b.to_address,
        b.transfer_count
    FROM base b
    LEFT JOIN `dbt`.`tokens_whitelist` w
      ON lower(w.address) = b.token_address
),

agg AS (
    SELECT
        date,
        token_address,
        any(symbol)      AS symbol,
        any(token_class) AS token_class,
        sum(amount)      AS volume_token,
        sum(amount_usd)  AS volume_usd,
        sum(transfer_count) AS transfer_count,
        groupBitmapState(cityHash64(from_address)) AS ua_bitmap_state,
        uniqExact(from_address)                    AS active_senders,
        uniqExact(to_address)                      AS unique_receivers
    FROM with_class
    GROUP BY date, token_address
)

SELECT
    date,
    token_address,
    symbol,
    token_class,
    volume_token,
    volume_usd,
    transfer_count,
    ua_bitmap_state,
    active_senders,
    unique_receivers
FROM agg
ORDER BY date, token_address