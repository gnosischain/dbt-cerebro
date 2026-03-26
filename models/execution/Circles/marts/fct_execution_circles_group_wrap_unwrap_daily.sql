{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'wrappers']
    )
}}

WITH wrapper_activity AS (
    SELECT
        toDate(wt.block_timestamp) AS date,
        w.avatar AS group_address,
        wt.token_address AS wrapper_address,
        w.circles_type,
        if(wt.from_address = {{ circles_zero_address() }}, wt.amount_raw, toUInt256(0)) AS wrapped_amount_raw,
        if(wt.to_address = {{ circles_zero_address() }}, wt.amount_raw, toUInt256(0)) AS unwrapped_amount_raw,
        if(wt.from_address = {{ circles_zero_address() }}, 1, 0) AS wrap_count,
        if(wt.to_address = {{ circles_zero_address() }}, 1, 0) AS unwrap_count
    FROM {{ ref('int_execution_circles_wrapper_transfers') }} wt
    INNER JOIN {{ ref('int_execution_circles_wrappers') }} w
        ON wt.token_address = w.wrapper_address
    INNER JOIN {{ ref('int_execution_circles_group_registrations') }} g
        ON w.avatar = g.group_address
    WHERE wt.from_address = {{ circles_zero_address() }}
       OR wt.to_address = {{ circles_zero_address() }}
)

SELECT
    date,
    group_address,
    wrapper_address,
    circles_type,
    sum(wrap_count) AS wrap_count,
    sum(wrapped_amount_raw) AS wrapped_amount_raw,
    sum(unwrap_count) AS unwrap_count,
    sum(unwrapped_amount_raw) AS unwrapped_amount_raw
FROM wrapper_activity
GROUP BY 1, 2, 3, 4
