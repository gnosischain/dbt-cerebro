{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(date, offer_address)',
    unique_key='(date, offer_address)',
    partition_by='toStartOfMonth(date)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','token_offer_funnel','daily']
  )
}}

-- Daily per-offer claim conversion. "Conversion rate" is computed as
--   n_claimers / n_eligible_pool
-- where eligible_pool is the rolling 30-day active GA users on each date.
-- This is an approximation — a true "eligible" set would require knowing
-- which addresses hold the source token at each moment. Documenting the
-- proxy explicitly here so it's not mistaken for a strict eligibility
-- denominator.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH active_pool_daily AS (
    -- Rolling 30-day active users as the eligible-pool proxy.
    SELECT
        date,
        uniqExact(address)        AS n_active_30d
    FROM (
        SELECT
            d.date,
            ua.address
        FROM (
            SELECT DISTINCT date
            FROM {{ ref('int_execution_gnosis_app_user_activity_daily') }}
            WHERE date < today()
              {% if start_month and end_month %}
                AND toStartOfMonth(date) >= toDate('{{ start_month }}')
                AND toStartOfMonth(date) <= toDate('{{ end_month }}')
              {% else %}
                {{ apply_monthly_incremental_filter(
                      source_field='date',
                      destination_field='date',
                      add_and=True,
                      lookback_days=1) }}
              {% endif %}
        ) d
        INNER JOIN {{ ref('int_execution_gnosis_app_user_activity_daily') }} ua
            ON ua.date >  d.date - 30
           AND ua.date <= d.date
           AND ua.activity_kind != 'onboard'
    )
    GROUP BY date
),

claims_daily AS (
    SELECT
        toDate(block_timestamp)                       AS date,
        offer_address                                 AS offer_address,
        count()                                       AS n_claims,
        uniqExact(ga_user)                            AS n_claimers,
        sum(toFloat64OrNull(toString(amount_received_usd))) AS amount_received_usd
    FROM {{ ref('int_execution_gnosis_app_token_offer_claims') }}
    WHERE block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
        AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter(
              source_field='block_timestamp',
              destination_field='date',
              add_and=True) }}
      {% endif %}
    GROUP BY date, offer_address
)

SELECT
    c.date                                                                AS date,
    c.offer_address                                                       AS offer_address,
    c.n_claims                                                            AS n_claims,
    c.n_claimers                                                          AS n_claimers,
    coalesce(c.amount_received_usd, 0)                                    AS amount_received_usd,
    coalesce(p.n_active_30d, 0)                                           AS n_active_pool_30d,
    round(
        toFloat64(c.n_claimers) / nullIf(toFloat64(p.n_active_30d), 0) * 100,
        2
    )                                                                     AS claim_rate_pct
FROM claims_daily c
LEFT JOIN active_pool_daily p ON p.date = c.date
