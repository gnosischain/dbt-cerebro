{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(conversion_date, conversion_kind, user_pseudonym, identity_role, conversion_ts, conversion_dedup_key)',
    unique_key='(conversion_ts, conversion_kind, user_pseudonym, identity_role, conversion_dedup_key)',
    partition_by='toStartOfMonth(conversion_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gpay'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- GP conversion registry. Like the GA registry, but each row also carries
-- `identity_role`. The persona filters by role at query time:
--   WHERE conversion_kind='gpay_payment' AND identity_role='initial_owner'
--   → owner-grain (one human → one row per owned Safe's payments)
--   WHERE conversion_kind='gpay_payment' AND identity_role='safe_self'
--   → treasury-grain (one Safe → one row per payment)
-- Both grains are present; persona picks at query time.
--
-- v1 conversion sources:
--   gpay_payment        — int_execution_gpay_activity WHERE action='Payment'
--   gpay_funded         — first time a Safe receives any inflow
--                        (action IN ('Fiat Top Up', 'Crypto Deposit'))
--   gpay_cashback_claim — int_execution_gpay_activity WHERE action='Cashback'

WITH bridge AS (
    SELECT address, user_pseudonym, identity_role, gp_safe
    FROM {{ ref('int_execution_gpay_user_identity_bridge') }}
),

activity AS (
    -- int_execution_gpay_activity has no log_index column; its unique
    -- key is (wallet_address, block_timestamp, transaction_hash,
    -- token_address, counterparty, direction). Build dedup_key from
    -- those columns instead.
    SELECT
        toDateTime(a.block_timestamp)             AS conversion_ts,
        toDate(a.block_timestamp)                 AS conversion_date,
        lower(a.wallet_address)                   AS gp_safe,
        a.transaction_hash,
        a.token_address,
        a.counterparty,
        a.direction,
        a.action,
        a.symbol,
        a.amount_usd
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.block_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(a.block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(a.block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('a.block_timestamp', 'conversion_date', add_and=True) }}
    {% endif %}
),

payments AS (
    SELECT
        a.conversion_ts,
        a.conversion_date,
        b.user_pseudonym,
        b.identity_role,
        'gpay_payment'                                                AS conversion_kind,
        toFloat64OrNull(toString(a.amount_usd))                       AS conversion_amount_usd,
        a.symbol                                                      AS conversion_token,
        cityHash64('gpay_payment', a.transaction_hash, a.token_address, a.counterparty, a.direction, b.identity_role) AS conversion_dedup_key,
        'int_execution_gpay_activity'                                 AS provenance_model
    FROM activity a
    INNER JOIN bridge b ON b.gp_safe = a.gp_safe
    WHERE a.action = 'Payment'
),

cashback_claims AS (
    SELECT
        a.conversion_ts,
        a.conversion_date,
        b.user_pseudonym,
        b.identity_role,
        'gpay_cashback_claim'                                         AS conversion_kind,
        toFloat64OrNull(toString(a.amount_usd))                       AS conversion_amount_usd,
        a.symbol                                                      AS conversion_token,
        cityHash64('gpay_cashback_claim', a.transaction_hash, a.token_address, a.counterparty, a.direction, b.identity_role) AS conversion_dedup_key,
        'int_execution_gpay_activity'                                 AS provenance_model
    FROM activity a
    INNER JOIN bridge b ON b.gp_safe = a.gp_safe
    WHERE a.action = 'Cashback'
),

-- gpay_funded = the FIRST inflow event into the Safe (wallet-level), per Safe.
-- We compute first_inflow_ts per gp_safe across the whole history (NOT
-- limited by the microbatch window, since "first" is a global property),
-- then filter to rows whose first_inflow_ts falls in the target window.
first_inflow AS (
    SELECT
        lower(wallet_address)                       AS gp_safe,
        min(block_timestamp)                        AS first_inflow_ts,
        argMin(transaction_hash, block_timestamp)   AS first_inflow_tx_hash,
        argMin(token_address,    block_timestamp)   AS first_inflow_token_address,
        argMin(counterparty,     block_timestamp)   AS first_inflow_counterparty,
        argMin(direction,        block_timestamp)   AS first_inflow_direction,
        argMin(symbol,           block_timestamp)   AS first_inflow_symbol,
        argMin(amount_usd,       block_timestamp)   AS first_inflow_amount_usd
    FROM {{ ref('int_execution_gpay_activity') }}
    WHERE action IN ('Fiat Top Up', 'Crypto Deposit')
    GROUP BY lower(wallet_address)
),

funded AS (
    SELECT
        toDateTime(fi.first_inflow_ts)                                AS conversion_ts,
        toDate(fi.first_inflow_ts)                                    AS conversion_date,
        b.user_pseudonym,
        b.identity_role,
        'gpay_funded'                                                 AS conversion_kind,
        toFloat64OrNull(toString(fi.first_inflow_amount_usd))         AS conversion_amount_usd,
        fi.first_inflow_symbol                                        AS conversion_token,
        cityHash64('gpay_funded', fi.first_inflow_tx_hash, fi.first_inflow_token_address, fi.first_inflow_counterparty, fi.first_inflow_direction, b.identity_role) AS conversion_dedup_key,
        'int_execution_gpay_activity'                                 AS provenance_model
    FROM first_inflow fi
    INNER JOIN bridge b ON b.gp_safe = fi.gp_safe
    WHERE fi.first_inflow_ts < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(fi.first_inflow_ts) >= toDate('{{ start_month }}')
      AND toStartOfMonth(fi.first_inflow_ts) <= toDate('{{ end_month }}')
    {% elif is_incremental() %}
      -- Incremental run: pull only new "first_inflow" rows since last run.
      -- {{ this }} doesn't exist on the initial full-refresh build, so the
      -- whole condition is gated by is_incremental().
      AND toStartOfMonth(fi.first_inflow_ts) >= (
        SELECT coalesce(toStartOfMonth(max(toDate(conversion_date)) - INTERVAL 1 MONTH),
                        toDate('1970-01-01'))
        FROM {{ this }}
        WHERE conversion_kind = 'gpay_funded'
      )
    {% endif %}
)

SELECT * FROM payments
UNION ALL SELECT * FROM cashback_claims
UNION ALL SELECT * FROM funded
