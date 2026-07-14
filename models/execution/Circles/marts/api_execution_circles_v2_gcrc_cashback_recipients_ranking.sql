{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_gcrc_cashback_recipients_ranking','granularity:latest']
    )
}}

-- Top lifetime recipients of Circles v2 gCRC cashback, with profile.
WITH agg AS (
    SELECT
        address,
        sum(amount) AS total_amount,
        count()     AS n_weeks,
        max(week)   AS last_week
    FROM {{ ref('int_execution_circles_v2_gcrc_cashback_recipients_weekly') }}
    WHERE week < toStartOfWeek(today(), 1)
    GROUP BY address
),
meta AS (
    SELECT
        avatar,
        argMax(display_name, registered_at)       AS display_name,
        argMax(preview_image_url, registered_at)  AS preview_image_url
    FROM (
        SELECT
            lower(avatar) AS avatar,
            coalesce(nullIf(metadata_name, ''), nullIf(name, ''), avatar) AS display_name,
            metadata_preview_image_url AS preview_image_url,
            registered_at
        FROM {{ ref('api_execution_circles_v2_avatar_metadata') }}
    )
    GROUP BY avatar
)
SELECT
    today() AS as_of_date,
    row_number() OVER (ORDER BY a.total_amount DESC) AS rank,
    a.address,
    coalesce(m.display_name, a.address) AS display_name,
    m.preview_image_url,
    a.total_amount,
    a.n_weeks,
    a.last_week
FROM agg a
LEFT JOIN meta m ON m.avatar = lower(a.address)
ORDER BY a.total_amount DESC
LIMIT 100
