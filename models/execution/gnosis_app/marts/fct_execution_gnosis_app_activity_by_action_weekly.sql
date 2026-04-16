{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(week, activity_kind)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','activity','mart']
  )
}}

{# Description in schema.yml — see fct_execution_gnosis_app_activity_by_action_weekly #}

SELECT
    toStartOfWeek(date, 1)                                              AS week,
    activity_kind                                                       AS activity_kind,
    sum(n_events)                                                       AS n_events,
    countDistinct(address)                                              AS n_users,
    sum(amount_usd)                                                     AS amount_usd
FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
GROUP BY week, activity_kind
ORDER BY week, activity_kind
