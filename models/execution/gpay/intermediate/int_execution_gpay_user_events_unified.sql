{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'delete+insert'),
    engine='ReplacingMergeTree()',
    order_by='(event_date, user_pseudonym, event_ts)',
    unique_key='(event_ts, event_kind, user_pseudonym, identity_role, event_dedup_key)',
    partition_by='toStartOfMonth(event_date)',
    settings={'allow_nullable_key': 1},
    tags=['production', 'mta', 'execution', 'gpay'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Long-form GP-side event log keyed by user_pseudonym, with identity_role
-- fan-out via the GP identity bridge. Each chain event on a GP Safe
-- produces one row per (user_pseudonym, identity_role) pair tied to that
-- Safe — so a payment from a 2-owner Safe yields 3 rows: safe_self +
-- two initial_owners.
--
-- v1 scope is chain-side only (int_execution_gpay_activity). For
-- cross-domain Mixpanel touchpoints on GP Safe owners, query the GA-side
-- int_execution_gnosis_app_user_events_unified directly via
-- user_pseudonym join — same hash space across sectors when the
-- underlying EOA matches.

WITH bridge AS (
    SELECT address, user_pseudonym, identity_role, gp_safe
    FROM {{ ref('int_execution_gpay_user_identity_bridge') }}
),

activity AS (
    -- int_execution_gpay_activity has no log_index; its unique key is
    -- (wallet_address, block_timestamp, transaction_hash, token_address,
    -- counterparty, direction). Use those for dedup_key.
    SELECT
        toDateTime(a.block_timestamp)                            AS event_ts,
        toDate(a.block_timestamp)                                AS event_date,
        lower(a.wallet_address)                                  AS gp_safe,
        a.transaction_hash,
        a.token_address,
        a.counterparty,
        a.symbol,
        a.action,
        a.direction,
        a.amount,
        a.amount_usd
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.block_timestamp < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(a.block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(a.block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('a.block_timestamp', 'event_date', add_and=True) }}
    {% endif %}
)

SELECT
    a.event_ts                                                   AS event_ts,
    a.event_date                                                 AS event_date,
    b.user_pseudonym                                             AS user_pseudonym,
    b.identity_role                                              AS identity_role,
    'chain'                                                      AS event_source,
    multiIf(
        a.action = 'Payment',           'gp.payment',
        a.action = 'Cashback',          'gp.cashback_claim',
        a.action = 'Fiat Top Up',       'gp.deposit',
        a.action = 'Fiat Off-ramp',     'gp.withdrawal',
        a.action = 'Crypto Deposit',    'gp.deposit',
        a.action = 'Crypto Withdrawal', 'gp.withdrawal',
        a.action = 'Reversal',          'gp.payment',
        'gp.action_other'
    )                                                            AS event_kind,
    a.action                                                     AS event_subkind,
    toFloat64OrNull(toString(a.amount_usd))                      AS amount_usd,
    cityHash64(a.transaction_hash, a.token_address, a.counterparty, a.direction, b.identity_role) AS event_dedup_key,
    'int_execution_gpay_activity'                                AS provenance_model
FROM activity a
INNER JOIN bridge b ON b.gp_safe = a.gp_safe
