{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='(date, bridge_contract, user_address)',
        unique_key=['date', 'bridge_contract', 'user_address', 'token_address', 'direction'],
        tags=["execution", "transfers", "bridges", "graph_explorer"]
    )
}}

-- Address-grain bridge flows. Joins whitelisted transfers against the dune
-- labels set filtered on bridge projects to get (bridge_contract, user,
-- token, direction, date). Enables the Graph Explorer to render an
-- (address -> bridge) edge that the aggregate chain-level bridge models do
-- not expose.

WITH bridge_addrs AS (
    SELECT
        lower(address) AS address
        , any(project) AS bridge_name
    FROM {{ ref('int_crawlers_data_labels') }}
    WHERE sector = 'Bridges'
       OR lowerUTF8(project) LIKE '%bridge%'
    GROUP BY address
),
legs AS (
    SELECT
        date
        , token_address
        , symbol
        , lower(ifNull(`from`, '')) AS from_address
        , lower(ifNull(`to`, '')) AS to_address
        , amount_raw
        , transfer_count
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }}
    {% if is_incremental() %}
    WHERE date > (SELECT max(date) FROM {{ this }})
    {% endif %}
)
SELECT
    l.date AS date
    , coalesce(ba_to.address, ba_from.address) AS bridge_contract
    , coalesce(ba_to.bridge_name, ba_from.bridge_name) AS bridge_name
    , l.token_address AS token_address
    , l.symbol AS symbol
    , if(ba_to.address IS NOT NULL, l.from_address, l.to_address) AS user_address
    , if(ba_to.address IS NOT NULL, 'out', 'in') AS direction
    , sum(toFloat64OrZero(toString(l.amount_raw))) AS amount_raw_sum
    , CAST(NULL AS Nullable(Float64)) AS volume_usd
    , sum(l.transfer_count) AS transfer_count
FROM legs l
LEFT JOIN bridge_addrs ba_to ON ba_to.address = l.to_address
LEFT JOIN bridge_addrs ba_from ON ba_from.address = l.from_address
WHERE ba_to.address IS NOT NULL OR ba_from.address IS NOT NULL
GROUP BY
    l.date
    , bridge_contract
    , bridge_name
    , l.token_address
    , l.symbol
    , user_address
    , direction
