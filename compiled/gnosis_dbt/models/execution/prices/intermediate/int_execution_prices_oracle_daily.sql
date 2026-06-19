

-- Native daily USD prices from Chainlink on-chain oracle feeds (AnswerUpdated), read
-- from the single combined decode model and split by aggregator address -> feed.
-- USD / forex feeds are 8-decimal (answer/1e8); wstETH-ETH is an 18-decimal exchange
-- rate -> USD = (answer/1e18) * ETH/USD. One feed can map to several whitelist symbols
-- (e.g. USDC/USD -> USDC + USDC.e). argMax over block_timestamp = last answer of day.
-- Native USD anchor that replaces the Dune price feed; see docs/native_token_prices_build_plan.md.




-- feed -> whitelist symbol(s). svZCHF ~= ZCHF; USDC.e ~= USDC; xDAI = WxDAI = DAI/USD.









































WITH decoded AS (
    SELECT
        multiIf(
            addr IN ('016a45f646bbd35b61fe7a496a75d9ea69bd243e','ca16ed36a7d1ae2dc68873d62bce4f9bdcc2d378'), 'GNO_USD',
            addr IN ('44513922bf52cec40a0557797b040805ded50140','059e7bd8157e0d302df3626e162b6c835340b311'), 'ETH_USD',
            addr =  '5ed6a59735297bc5d6cb4942913ae7098e0cd703',  'WBTC_USD',
            addr =  '759be90a34e426042ed7d17916b78a5cd2567dd1',  'EUR_USD',
            addr IN ('be18b8f41760878ba6d3b1e9475c4ccad3d9aa8f','6e2482e011ec31a1960a938791b6b4ff5baa3217'), 'CHF_USD',
            addr =  '6dcf8ce1982fc71e7128407c7c6ce4b0c1722f55',  'wstETH_ETH',
            addr IN ('c15288bc7e921dc462d9c4ce151318d5aa428a53','30ba871ee7a08dbd255cdd8e7e035dad72014e27'), 'USDC_USD',
            addr =  'c4d924b6bab6fec909e482b93847d997463f0c79',  'USDT_USD',
            addr IN ('12a6b73a568f8dc3d24da1654079343f18f69236','b65566283cace6b281308308da0f0783a613c416'), 'DAI_USD',
            ''
        )                                                                               AS feed,
        if(addr = '6dcf8ce1982fc71e7128407c7c6ce4b0c1722f55', 18, 8)                    AS n_dec,
        toStartOfDay(block_timestamp)                                                  AS date,
        toFloat64(toInt256OrNull(decoded_params['current']))                           AS answer_raw,
        block_timestamp
    FROM (
        SELECT
            replaceAll(lower(contract_address), '0x', '') AS addr,
            block_timestamp,
            decoded_params
        FROM `dbt`.`contracts_chainlink_feeds_events`
        WHERE event_name = 'AnswerUpdated'
          AND block_timestamp < today()
          
            
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_prices_oracle_daily` AS x1
        WHERE 1=1 
      )
      
    
  

          
    )
),

feed_daily AS (
    SELECT
        feed,
        date,
        argMax(answer_raw / POWER(10, n_dec), block_timestamp) AS answer
    FROM decoded
    WHERE feed != ''
    GROUP BY feed, date
),

eth_daily AS (
    SELECT date, answer AS eth_usd FROM feed_daily WHERE feed = 'ETH_USD'
),

feed_usd AS (
    -- direct USD / forex feeds: answer is already USD
    SELECT feed, date, answer AS usd_price
    FROM feed_daily
    WHERE feed != 'wstETH_ETH'

    UNION ALL

    -- exchange-rate feeds: answer (in ETH) * same-day ETH/USD
    SELECT r.feed, r.date, r.answer * e.eth_usd AS usd_price
    FROM feed_daily r
    INNER JOIN eth_daily e ON e.date = r.date
    WHERE r.feed = 'wstETH_ETH'
)

SELECT symbol, date, price
FROM (
    SELECT 'GNO' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'GNO_USD'
    UNION ALL
SELECT 'WETH' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'ETH_USD'
    UNION ALL
SELECT 'WBTC' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'WBTC_USD'
    UNION ALL
SELECT 'EURe' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'EUR_USD'
    UNION ALL
SELECT 'ZCHF' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'CHF_USD'
    UNION ALL
SELECT 'svZCHF' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'CHF_USD'
    UNION ALL
SELECT 'USDC' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'USDC_USD'
    UNION ALL
SELECT 'USDC.e' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'USDC_USD'
    UNION ALL
SELECT 'USDT' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'USDT_USD'
    UNION ALL
SELECT 'xDAI' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'DAI_USD'
    UNION ALL
SELECT 'WxDAI' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'DAI_USD'
    UNION ALL
SELECT 'wstETH' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = 'wstETH_ETH'
)
WHERE price > 0