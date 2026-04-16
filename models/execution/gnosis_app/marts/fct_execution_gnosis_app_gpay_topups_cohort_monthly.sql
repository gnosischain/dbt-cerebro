{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(cohort_month, activity_month)',
    tags=['production','execution','gnosis_app','gpay','topups','retention','mart']
  )
}}

{# Description in schema.yml — see fct_execution_gnosis_app_gpay_topups_cohort_monthly #}

{#
  First-TopUp cohort × subsequent TopUp activity retention. Same shape as
  fct_execution_gpay_cashback_cohort_retention_monthly, but for GA topups.
  Grain (cohort_month, activity_month). retention_pct = users_in_activity_month
  / initial_cohort_size.
#}

WITH first_topup AS (
    SELECT
        ga_user                            AS address,
        toStartOfMonth(min(block_timestamp)) AS cohort_month
    FROM {{ ref('int_execution_gnosis_app_gpay_topups') }}
    GROUP BY ga_user
),

monthly_topup AS (
    SELECT
        ga_user                            AS address,
        toStartOfMonth(block_timestamp)    AS activity_month,
        sum(amount_usd)                    AS amount_usd
    FROM {{ ref('int_execution_gnosis_app_gpay_topups') }}
    WHERE toStartOfMonth(block_timestamp) < toStartOfMonth(today())
    GROUP BY ga_user, activity_month
),

cohort_activity AS (
    SELECT
        f.cohort_month,
        a.activity_month,
        dateDiff('month', f.cohort_month, a.activity_month) AS months_since,
        count(DISTINCT a.address)                           AS users,
        sum(a.amount_usd)                                   AS amount_usd
    FROM first_topup f
    INNER JOIN monthly_topup a ON f.address = a.address
    GROUP BY f.cohort_month, a.activity_month
),

with_initial AS (
    SELECT
        *,
        max(users) OVER (PARTITION BY cohort_month)                       AS initial_users,
        argMin(amount_usd, activity_month) OVER (PARTITION BY cohort_month) AS initial_amount_usd
    FROM cohort_activity
)

SELECT
    cohort_month,
    activity_month,
    months_since,
    users,
    initial_users,
    round(users / greatest(initial_users, 1) * 100, 1)                  AS retention_pct,
    round(amount_usd / nullIf(initial_amount_usd, 0) * 100, 1)          AS amount_retention_pct,
    round(toFloat64(amount_usd), 2)                                     AS amount_usd
FROM with_initial
ORDER BY cohort_month, activity_month
