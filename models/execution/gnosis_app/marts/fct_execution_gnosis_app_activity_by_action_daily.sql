{{
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='(date, activity_kind)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','activity','mart']
  )
}}

{# Description in schema.yml — see fct_execution_gnosis_app_activity_by_action_daily #}

SELECT
    date                                                                AS date,
    activity_kind                                                       AS activity_kind,
    sum(n_events)                                                       AS n_events,
    countDistinct(address)                                              AS n_users,
    sum(amount_usd)                                                     AS amount_usd
FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
GROUP BY date, activity_kind
ORDER BY date, activity_kind
