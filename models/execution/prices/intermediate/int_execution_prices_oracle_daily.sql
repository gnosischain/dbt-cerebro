{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(date, symbol)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        pre_hook=[
            "SET max_memory_usage = 6000000000",
            "SET max_bytes_before_external_group_by = 2000000000"
        ],
        post_hook=[
            "SET max_memory_usage = 0",
            "SET max_bytes_before_external_group_by = 0"
        ],
        tags=['production','execution','prices','oracle','intermediate','granularity:daily']
    )
}}

-- Native daily USD prices from Chainlink on-chain oracle feeds (AnswerUpdated), read
-- from the single combined decode model and split by aggregator address -> feed.
-- USD / forex feeds are 8-decimal (answer/1e8); wstETH-ETH is an 18-decimal exchange
-- rate -> USD = (answer/1e18) * ETH/USD. One feed can map to several whitelist symbols
-- (e.g. USDC/USD -> USDC + USDC.e). argMax over block_timestamp = last answer of day.
-- Native USD anchor that replaces the Dune price feed; see docs/native_token_prices_build_plan.md.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- feed -> whitelist symbol(s). svZCHF ~= ZCHF; USDC.e ~= USDC; xDAI = WxDAI = DAI/USD.
{% set feed_symbols = [
    ('GNO_USD',    'GNO'),
    ('ETH_USD',    'WETH'),
    ('WBTC_USD',   'WBTC'),
    ('EUR_USD',    'EURe'),
    ('CHF_USD',    'ZCHF'),
    ('CHF_USD',    'svZCHF'),
    ('USDC_USD',   'USDC'),
    ('USDC_USD',   'USDC.e'),
    ('USDT_USD',   'USDT'),
    ('DAI_USD',    'xDAI'),
    ('DAI_USD',    'WxDAI'),
    ('wstETH_ETH', 'wstETH')
] %}

{% set map_parts = [] %}
{% for feed, sym in feed_symbols %}
{% set mp %}SELECT '{{ sym }}' AS symbol, date, usd_price AS price FROM feed_usd WHERE feed = '{{ feed }}'{% endset %}
{% do map_parts.append(mp) %}
{% endfor %}

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
        FROM {{ ref('contracts_chainlink_feeds_events') }}
        WHERE event_name = 'AnswerUpdated'
          AND block_timestamp < today()
          {% if start_month and end_month %}
            AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
            AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
          {% else %}
            {{ apply_monthly_incremental_filter('block_timestamp', 'date', 'true') }}
          {% endif %}
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
    {{ map_parts | join('\n    UNION ALL\n') }}
)
WHERE price > 0
