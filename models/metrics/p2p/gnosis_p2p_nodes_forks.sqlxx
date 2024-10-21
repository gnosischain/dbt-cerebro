{{ config(
    materialized='incremental',
    unique_key=['datetime', 'fork'],
    incremental_strategy='delete+insert'
) }}

{{ node_count_over_time(
    group_by_column="next_fork_label",
    source_model=ref('gnosis_p2p_nodes_status'),
    grouping_column_name='fork'
) }}