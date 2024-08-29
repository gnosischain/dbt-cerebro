{{ config(
    materialized='incremental',
    unique_key=['datetime', 'country_code'],
    incremental_strategy='delete+insert'
) }}

{{ node_count_over_time(
    group_by_column="COALESCE(geo_country_code, 'Unknown')",
    source_model=ref('gnosis_p2p_nodes_status'),
    grouping_column_name='country_code'
) }}