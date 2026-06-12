{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, container_address, ubo_address, token_address)',
        unique_key='(date, protocol, container_address, ubo_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production','execution','ubo','claims','supply_claims']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

WITH

kc AS (
    SELECT DISTINCT date, container_address, token_address
    FROM {{ ref('fct_ubo_known_containers_daily') }}
),

-- Direct holders: pass through unchanged, stripping rows where ubo_address
-- is itself a known container for the bridge token.
clean AS (
    SELECT f.*
    FROM {{ ref('fct_ubo_supply_claims_daily') }} f
    LEFT JOIN kc
        ON  f.date              = kc.date
        AND f.ubo_address       = kc.container_address
        AND f.container_address = kc.token_address
    WHERE kc.container_address IS NULL
      AND f.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(f.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(f.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('f.date', 'date', 'true') }}
      {% endif %}
),

-- Second-level redistribution: stream the full claims table through on the
-- left (probe side) and join the tiny pre-materialized second-level rows on
-- the right (hash-table side). All column references are fully qualified —
-- no bare names in the CTE SELECT scope — which avoids the ClickHouse
-- UNKNOWN_IDENTIFIER error that fires when both join sides share column names
-- and the outer SELECT of a CTE tries to resolve bare references.
redistributed AS (
    SELECT
        sub.date                                                                        AS date,
        s.protocol                                                                      AS protocol,
        s.container_address                                                             AS container_address,
        s.token_address                                                                 AS token_address,
        s.symbol                                                                        AS symbol,
        s.token_class                                                                   AS token_class,
        sub.ubo_address                                                                 AS ubo_address,
        toInt256(round(
            sub.balance
            / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
            * toFloat64(s.balance_raw)
        ))                                                                              AS balance_raw,
        sub.balance
        / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
        * s.balance                                                                     AS balance,
        sub.balance
        / nullIf(sum(sub.balance) OVER (PARTITION BY sub.date, sub.container_address, sub.token_address), 0)
        * s.balance_usd                                                                 AS balance_usd
    FROM {{ ref('fct_ubo_supply_claims_daily') }} sub
    INNER JOIN {{ ref('int_ubo_second_level_daily') }} s
        ON  sub.date              = s.date
        AND sub.container_address = s.ubo_address
        AND sub.token_address     = s.container_address
    WHERE sub.date < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(sub.date) >= toDate('{{ start_month }}')
        AND toStartOfMonth(sub.date) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('sub.date', 'date', 'true') }}
      {% endif %}
)

SELECT
    date,
    protocol,
    container_address,
    token_address,
    any(symbol)                AS symbol,
    any(token_class)           AS token_class,
    ubo_address,
    toInt256(sum(balance_raw)) AS balance_raw,
    sum(balance)               AS balance,
    sum(balance_usd)           AS balance_usd
FROM (
    SELECT * FROM clean
    UNION ALL
    SELECT * FROM redistributed
)
GROUP BY date, protocol, container_address, token_address, ubo_address
SETTINGS max_bytes_before_external_group_by = 3000000000
