{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if (var('start_month', none) or var('incremental_end_date', none)) else 'delete+insert'),
        on_schema_change='sync_all_columns',
        engine='ReplacingMergeTree()',
        order_by='(date, container_address, token_address)',
        unique_key='(date, container_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev','execution','ubo','known_containers']
    )
}}

-- Distinct list of (date, container_address, token_address) tuples for
-- which we have UBO-level supply claims. Derived directly from
-- fct_ubo_supply_claims_daily so it stays self-consistent: a container
-- appears here iff we can decompose it.
--
-- Downstream consumers (top holders, UBO coverage) LEFT ANTI JOIN this
-- against balances_daily to strip out container-level rows before merging
-- in the per-UBO rows from fct_ubo_supply_claims_daily. Small — bounded by
-- (# protocols × # reserves × days) — so refresh is cheap.

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

SELECT DISTINCT
    date,
    container_address,
    token_address
FROM {{ ref('fct_ubo_supply_claims_daily') }}
WHERE date < today()
  {% if start_month and end_month %}
    AND toStartOfMonth(date) >= toDate('{{ start_month }}')
    AND toStartOfMonth(date) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter('date', 'date', 'true') }}
  {% endif %}
