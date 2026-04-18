{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(holder_type, date)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'supply_daily'],
        pre_hook=["SET join_use_nulls = 1"],
        post_hook=["SET join_use_nulls = 0"]
    )
}}
WITH balance_addresses AS (
    SELECT DISTINCT account AS address
    FROM {{ ref('int_execution_circles_v2_balances_daily') }}
    WHERE account != '0x0000000000000000000000000000000000000000'
      AND balance_raw > toInt256(0)
),
labels_ranked AS (
    SELECT
        address,
        project,
        row_number() OVER (
            PARTITION BY address
            ORDER BY introduced_at DESC, project DESC, sector DESC
        ) AS rn
    FROM {{ ref('int_crawlers_data_labels') }}
    WHERE address IN (SELECT address FROM balance_addresses)
),
labels AS (
    SELECT address, project
    FROM labels_ranked
    WHERE rn = 1
),
avatars_dedup AS (
    SELECT
        avatar,
        argMax(avatar_type, block_timestamp) AS avatar_type
    FROM {{ ref('int_execution_circles_v2_avatars') }}
    GROUP BY avatar
)

SELECT
    b.date,
    coalesce(
        a.avatar_type,
        l.project,
        'Other'
    ) AS holder_type,
    sum(toFloat64(b.balance_raw) / 1e18) AS supply,
    sum(toFloat64(b.demurraged_balance_raw) / 1e18) AS demurraged_supply,
    countDistinct(b.account) AS holder_count
FROM {{ ref('int_execution_circles_v2_balances_daily') }} b
LEFT JOIN avatars_dedup a
    ON b.account = a.avatar
LEFT JOIN labels l
    ON b.account = l.address
WHERE b.account != '0x0000000000000000000000000000000000000000'
  AND b.balance_raw > toInt256(0)
GROUP BY 1, 2
ORDER BY 1, 2
