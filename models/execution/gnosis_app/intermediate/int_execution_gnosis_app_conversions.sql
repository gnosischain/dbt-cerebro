{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(conversion_date, conversion_kind, user_pseudonym, conversion_ts, conversion_dedup_key)',
    unique_key='(conversion_ts, conversion_kind, user_pseudonym, conversion_dedup_key)',
    partition_by='toStartOfMonth(conversion_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gnosis_app']
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Conversion registry — one row per conversion event, conversion_kind as
-- a column. The MTA persona's runtime mapping points at this as
-- `conversion_model`. Persona filters with WHERE conversion_kind = '<kind>'
-- to swap between conversion targets without changing SQL shape.

WITH bridge AS (
    SELECT address, user_pseudonym
    FROM {{ ref('int_execution_gnosis_app_user_identity_bridge') }}
),

topup_rows AS (
    SELECT
        toDateTime(t.block_timestamp)                        AS conversion_ts,
        toDate(t.block_timestamp)                            AS conversion_date,
        b.user_pseudonym                                     AS user_pseudonym,
        'topup'                                              AS conversion_kind,
        toFloat64OrNull(toString(t.amount_usd))              AS conversion_amount_usd,
        t.token_bought_symbol                                AS conversion_token,
        cityHash64('topup', t.transaction_hash, toString(t.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_gpay_topups'               AS provenance_model
    FROM {{ ref('int_execution_gnosis_app_gpay_topups') }} t
    INNER JOIN bridge b ON b.address = lower(t.ga_user)
    WHERE 1=1
    {% if start_month and end_month %}
      AND toStartOfMonth(t.block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(t.block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('t.block_timestamp', 'conversion_date', add_and=True) }}
    {% endif %}
),

swap_filled_rows AS (
    SELECT
        toDateTime(assumeNotNull(s.first_fill_at))               AS conversion_ts,
        toDate(assumeNotNull(s.first_fill_at))                   AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'swap_filled'                                            AS conversion_kind,
        toFloat64OrNull(toString(s.amount_usd))                  AS conversion_amount_usd,
        CAST(NULL AS Nullable(String))                           AS conversion_token,
        cityHash64('swap_filled', s.order_uid)                   AS conversion_dedup_key,
        'int_execution_gnosis_app_swaps'                         AS provenance_model
    FROM {{ ref('int_execution_gnosis_app_swaps') }} s
    INNER JOIN bridge b ON b.address = lower(s.taker)
    WHERE s.was_filled = 1
      AND s.first_fill_at IS NOT NULL
      AND s.first_fill_at < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(s.first_fill_at) >= toDate('{{ start_month }}')
      AND toStartOfMonth(s.first_fill_at) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('s.first_fill_at', 'conversion_date', add_and=True) }}
    {% endif %}
),

claim_rows AS (
    SELECT
        toDateTime(tc.block_timestamp)                           AS conversion_ts,
        toDate(tc.block_timestamp)                               AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'token_offer_claim'                                      AS conversion_kind,
        toFloat64OrNull(toString(tc.amount_received_usd))        AS conversion_amount_usd,
        tc.offer_token_symbol                                    AS conversion_token,
        cityHash64('token_offer_claim', tc.transaction_hash, toString(tc.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_token_offer_claims'            AS provenance_model
    FROM {{ ref('int_execution_gnosis_app_token_offer_claims') }} tc
    INNER JOIN bridge b ON b.address = lower(tc.ga_user)
    WHERE tc.block_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(tc.block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(tc.block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('tc.block_timestamp', 'conversion_date', add_and=True) }}
    {% endif %}
),

marketplace_rows AS (
    SELECT
        toDateTime(mp.block_timestamp)                           AS conversion_ts,
        toDate(mp.block_timestamp)                               AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'marketplace_buy'                                        AS conversion_kind,
        CAST(NULL AS Nullable(Float64))                          AS conversion_amount_usd,
        mp.offer_name                                            AS conversion_token,
        cityHash64('marketplace_buy', mp.transaction_hash, toString(mp.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_marketplace_payments'          AS provenance_model
    FROM {{ ref('int_execution_gnosis_app_marketplace_payments') }} mp
    INNER JOIN bridge b ON b.address = lower(mp.payer)
    WHERE mp.block_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(mp.block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(mp.block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('mp.block_timestamp', 'conversion_date', add_and=True) }}
    {% endif %}
)

SELECT * FROM topup_rows
UNION ALL SELECT * FROM swap_filled_rows
UNION ALL SELECT * FROM claim_rows
UNION ALL SELECT * FROM marketplace_rows
