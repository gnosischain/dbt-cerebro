{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH mint_single AS (
    SELECT
        toStartOfHour(block_timestamp) AS hour,
        lower(decoded_params['group']) AS group_address,
        toUInt256OrZero(decoded_params['value']) AS amount_raw,
        1 AS mint_event_count,
        0 AS redeem_event_count,
        toUInt256(0) AS redeem_amount_raw
    FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
    WHERE event_name = 'CollateralLockedSingle'
),
mint_batch AS (
    SELECT
        toStartOfHour(block_timestamp) AS hour,
        lower(decoded_params['group']) AS group_address,
        sum(toUInt256(batch_value)) AS amount_raw,
        1 AS mint_event_count,
        0 AS redeem_event_count,
        toUInt256(0) AS redeem_amount_raw
    FROM (
        SELECT
            block_timestamp,
            decoded_params,
            arrayJoin(JSONExtract(decoded_params['values'], 'Array(String)')) AS batch_value
        FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
        WHERE event_name = 'CollateralLockedBatch'
    )
    GROUP BY 1, 2
),
redeems AS (
    SELECT
        toStartOfHour(block_timestamp) AS hour,
        lower(decoded_params['group']) AS group_address,
        toUInt256(0) AS amount_raw,
        0 AS mint_event_count,
        1 AS redeem_event_count,
        toUInt256OrZero(decoded_params['value']) AS redeem_amount_raw
    FROM {{ ref('contracts_circles_v2_StandardTreasury_events') }}
    WHERE event_name = 'GroupRedeem'
),
unioned AS (
    SELECT * FROM mint_single
    UNION ALL
    SELECT * FROM mint_batch
    UNION ALL
    SELECT * FROM redeems
)

SELECT
    hour,
    group_address,
    sum(mint_event_count) AS mint_event_count,
    sum(amount_raw) AS mint_amount_raw,
    sum(redeem_event_count) AS redeem_event_count,
    sum(redeem_amount_raw) AS redeem_amount_raw
FROM unioned
GROUP BY 1, 2
