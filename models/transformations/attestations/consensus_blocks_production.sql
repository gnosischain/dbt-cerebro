{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='day'
    ) 
}}

WITH genesis AS (
    SELECT f_time AS genesis_time
    FROM {{ get_postgres('gnosis_chaind', 't_genesis') }}
    LIMIT 1
),
blocks AS (
    SELECT
        toDate({{ compute_timestamp_at_slot('f_slot') }}) AS day,
        MIN(f_slot) AS f_slot_start,
        CAST(SUM(IF(NOT f_canonical, 1, 0)) AS Int64) AS forked,
        CAST(SUM(IF(f_canonical IS NULL, 1, 0)) AS Int64) AS proposed
    FROM
        {{ get_postgres('gnosis_chaind', 't_blocks') }}
    {% if is_incremental() %}
    WHERE toDate({{ compute_timestamp_at_slot('f_slot') }}) >= (SELECT max(day) FROM {{ this }})
    {% endif %}
    GROUP BY 1
),
chain_specs AS (
    SELECT
        toInt64OrZero({{ get_chain_spec('SECONDS_PER_SLOT') }}) AS seconds_per_slot,
        {{ seconds_until_end_of_day('genesis.genesis_time') }} AS seconds_until_end_of_day
    FROM genesis
)

SELECT day, forked AS cnt, 'forked' AS label 
FROM blocks

UNION ALL

SELECT day, proposed AS cnt, 'proposed' AS label 
FROM blocks

UNION ALL

SELECT 
    blocks.day, 
    CASE 
        WHEN blocks.f_slot_start = 0 
            THEN CAST(
                (chain_specs.seconds_until_end_of_day) / chain_specs.seconds_per_slot - (blocks.proposed + blocks.forked) AS Int64
            )
        ELSE
            CAST(
                (24 * 3600) / chain_specs.seconds_per_slot - (blocks.proposed + blocks.forked) AS Int64
            ) 
    END AS cnt,
    'missed' AS label 
FROM blocks
CROSS JOIN chain_specs