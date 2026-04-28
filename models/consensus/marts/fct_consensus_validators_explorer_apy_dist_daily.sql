{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(withdrawal_credentials, date)',
        unique_key='(date, withdrawal_credentials)',
        partition_by='toStartOfMonth(date)',
        tags=["production", "consensus", "fct:validators_apy", "granularity:daily"]
    )
}}

-- Materialised passthrough of int_consensus_validators_explorer_apy_dist_daily so the
-- dashboard's `WHERE withdrawal_credentials = 'x'` filter prunes at read time via the
-- physical primary index. Same pattern as fct_consensus_validators_explorer_daily.

SELECT * FROM {{ ref('int_consensus_validators_explorer_apy_dist_daily') }}
{% if start_month and end_month %}
WHERE toStartOfMonth(date) >= toDate('{{ start_month }}')
  AND toStartOfMonth(date) <= toDate('{{ end_month }}')
{% endif %}
