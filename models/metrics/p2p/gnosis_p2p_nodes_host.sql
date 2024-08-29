{{ config(
    materialized='incremental',
    unique_key=['datetime', 'provider'],
    incremental_strategy='delete+insert'
) }}

{{ node_count_over_time(
    group_by_column="provider",
    source_model=ref('gnosis_p2p_nodes_status'),
    grouping_column_name='provider',
) }}