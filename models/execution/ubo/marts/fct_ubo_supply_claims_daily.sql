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

-- THE reusable UBO supply-claims surface.
--
-- One row per (date, protocol, container_address, ubo_address). A "supply
-- claim" is a withdrawable position an end-holder has on a token that is
-- held by a container contract (aToken, vault, pool LP). Joining this
-- model lets downstream consumers see real beneficial owners in place of
-- pool contracts.
--
-- Phase 1: Aave V3 + SparkLend.
-- Phase 2: Balancer V2.
-- Phase 3+: add UNION ALL branches for Balancer V3, Curve, V2-style AMMs.
-- Consumers downstream do not change — they pick up new protocols automatically.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

SELECT
    date,
    protocol,
    container_address,
    token_address,
    symbol,
    token_class,
    ubo_address,
    balance_raw,
    balance,
    balance_usd
FROM {{ ref('int_ubo_claims_aave_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}

UNION ALL

SELECT
    date,
    protocol,
    container_address,
    token_address,
    symbol,
    token_class,
    ubo_address,
    balance_raw,
    balance,
    balance_usd
FROM {{ ref('int_ubo_claims_balancer_v2_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}
