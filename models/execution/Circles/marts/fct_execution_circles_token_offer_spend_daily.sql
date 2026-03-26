{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'offers']
    )
}}

SELECT
    toDate(block_timestamp) AS date,
    cycle_address,
    count() AS claim_count,
    sum(toUInt256OrZero(decoded_params['spent'])) AS total_spent_raw,
    sum(toUInt256OrZero(decoded_params['received'])) AS total_received_raw
FROM {{ ref('int_execution_circles_offer_cycles') }}
WHERE event_name = 'OfferClaimed'
GROUP BY 1, 2
