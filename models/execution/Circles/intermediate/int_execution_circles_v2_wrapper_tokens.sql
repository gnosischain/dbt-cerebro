{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='wrapper_address',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'tokens']
    )
}}

SELECT
    w.wrapper_address,
    w.avatar,
    w.circles_type,
    coalesce(
        nullIf(splitByString('_0x', dl.project_raw)[1], ''),
        concat('CRC-', substring(w.wrapper_address, 3, 5))
    ) AS symbol
FROM {{ ref('int_execution_circles_v2_wrappers') }} w
LEFT JOIN {{ ref('stg_crawlers_data__dune_labels') }} dl
    ON dl.address = w.wrapper_address
