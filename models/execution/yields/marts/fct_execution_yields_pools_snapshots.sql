{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(token, metric)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'pools', 'snapshots']
    )
}}


SELECT
    token,
    metric,
    value,
    change_pct
FROM (
    WITH
    token_latest_dates AS (
        SELECT
            token,
            max(date) AS token_max_date
        FROM {{ ref('fct_execution_yields_pools_daily') }}
        WHERE date < today()
          AND token IS NOT NULL
          AND token != ''
        GROUP BY token
    ),

    tvl_latest AS (
        SELECT
            f.token,
            sum(f.tvl_usd) AS tvl_usd
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        INNER JOIN token_latest_dates tld ON tld.token = f.token AND f.date = tld.token_max_date
        WHERE f.token IS NOT NULL
          AND f.token != ''
        GROUP BY f.token
    ),

    tvl_7d_ago AS (
        SELECT
            f.token,
            sum(f.tvl_usd) AS tvl_usd
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        INNER JOIN token_latest_dates tld ON tld.token = f.token AND f.date = tld.token_max_date - INTERVAL 7 DAY
        WHERE f.token IS NOT NULL
          AND f.token != ''
        GROUP BY f.token
    ),

    fees_7d AS (
        SELECT
            f.token,
            sum(f.fees_usd_daily) AS fees_usd
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        INNER JOIN token_latest_dates tld ON tld.token = f.token
        WHERE f.date > tld.token_max_date - INTERVAL 7 DAY
          AND f.date <= tld.token_max_date
          AND f.token IS NOT NULL
          AND f.token != ''
        GROUP BY f.token
    ),

    fees_prior_7d AS (
        SELECT
            f.token,
            sum(f.fees_usd_daily) AS fees_usd
        FROM {{ ref('fct_execution_yields_pools_daily') }} f
        INNER JOIN token_latest_dates tld ON tld.token = f.token
        WHERE f.date > tld.token_max_date - INTERVAL 14 DAY
          AND f.date <= tld.token_max_date - INTERVAL 7 DAY
          AND f.token IS NOT NULL
          AND f.token != ''
        GROUP BY f.token
    ),

    combined AS (
        SELECT
            tld.token AS token,
            tl.tvl_usd AS tvl_latest,
            t7.tvl_usd AS tvl_7d_ago,
            f7.fees_usd AS fees_7d,
            fp.fees_usd AS fees_prior_7d
        FROM token_latest_dates tld
        LEFT JOIN tvl_latest tl ON tl.token = tld.token
        LEFT JOIN tvl_7d_ago t7 ON t7.token = tld.token
        LEFT JOIN fees_7d f7 ON f7.token = tld.token
        LEFT JOIN fees_prior_7d fp ON fp.token = tld.token
    )

    SELECT
        token,
        'TVL_Latest' AS metric,
        tvl_latest AS value,
        round(
            CASE
                WHEN tvl_7d_ago IS NULL OR tvl_7d_ago = 0 THEN NULL
                ELSE ((tvl_latest / tvl_7d_ago) - 1) * 100
            END,
            2
        ) AS change_pct
    FROM combined
    WHERE tvl_latest IS NOT NULL

    UNION ALL

    SELECT
        token,
        'Fees_7D' AS metric,
        fees_7d AS value,
        round(
            CASE
                WHEN fees_prior_7d IS NULL OR fees_prior_7d = 0 THEN NULL
                ELSE ((fees_7d / fees_prior_7d) - 1) * 100
            END,
            2
        ) AS change_pct
    FROM combined
    WHERE fees_7d IS NOT NULL
)
