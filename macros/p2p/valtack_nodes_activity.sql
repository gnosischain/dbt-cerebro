{% macro valtack_nodes_activity(resolution='day', aggregation_column=none) %}

{% set valid_resolutions = ['hour', 'day', 'week', 'month'] %}

{% if resolution not in valid_resolutions %}
    {{ exceptions.raise_compiler_error("Invalid resolution. Choose from: " ~ valid_resolutions | join(', ')) }}
{% endif %}

{% set valid_columns = ['city', 'country', 'asn_type', 'asn_organization'] %}

{% if aggregation_column is not none and aggregation_column not in valid_columns %}
    {{ exceptions.raise_compiler_error("Invalid aggregation column. Choose from: " ~ valid_columns | join(', ')) }}
{% endif %}

{{ config(
    materialized='incremental',
    unique_key='date' if aggregation_column is none else ['date', aggregation_column],
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

WITH 

combined_events AS (
    SELECT
        pe.enr,
        pe.ip,
        me.timestamp AS timestamp
    FROM 
        {{ source('valtrack', 'peer_discovered_events') }} pe
    INNER JOIN
        {{ source('valtrack','metadata_received_events') }} me
        ON pe.enr = me.enr
    WHERE
        me.timestamp < date_trunc('{{ resolution }}', now())
        {% if is_incremental() %}
            AND me.timestamp >= (SELECT max(date) FROM {{ this }})
        {% endif %}
),

{% if aggregation_column is not none %}
nodes_data AS (
    SELECT
        ce.timestamp,
        ce.enr,
        ce.ip,
        im.city,
        im.country,
        im.latitude,
        im.longitude,
        im.asn_organization,
        im.asn_type
    FROM
        combined_events ce
    INNER JOIN 
        {{ source('valtrack','ip_metadata') }} im
        ON ce.ip = im.ip
),
{% endif %}

date_series AS (
    SELECT arrayJoin(arrayMap(
        d -> date_trunc('{{ resolution }}', toDateTime(min(timestamp))) + INTERVAL d {{ resolution }},
        range(toUInt32(dateDiff('{{ resolution }}', date_trunc('{{ resolution }}', toDateTime(min(timestamp))), date_trunc('{{ resolution }}', now()))))
    )) AS date
    FROM {% if aggregation_column is not none %}nodes_data{% else %}combined_events{% endif %}
),

{% if aggregation_column is not none %}
distinct_values AS (
    SELECT DISTINCT {{ aggregation_column }}
    FROM nodes_data
),

date_value_combinations AS (
    SELECT 
        ds.date,
        dv.{{ aggregation_column }}
    FROM
        date_series ds
    CROSS JOIN
        distinct_values dv
),
{% endif %}

active_nodes AS (
    SELECT 
        date_trunc('{{ resolution }}', timestamp) AS date,
        {% if aggregation_column is not none %}
        {{ aggregation_column }},
        {% endif %}
        countDistinct(enr) AS active_node_count
    FROM {% if aggregation_column is not none %}nodes_data{% else %}combined_events{% endif %}
    GROUP BY date{% if aggregation_column is not none %}, {{ aggregation_column }}{% endif %}
)

SELECT 
    {% if aggregation_column is not none %}
    dvc.date,
    dvc.{{ aggregation_column }},
    {% else %}
    ds.date,
    {% endif %}
    COALESCE(an.active_node_count, 0) AS active_nodes
FROM 
    {% if aggregation_column is not none %}
    date_value_combinations dvc
    LEFT JOIN 
        active_nodes an 
        ON dvc.date = an.date AND dvc.{{ aggregation_column }} = an.{{ aggregation_column }}
    {% else %}
    date_series ds
    LEFT JOIN 
        active_nodes an 
        ON ds.date = an.date
    {% endif %}
WHERE 
    {% if aggregation_column is not none %}dvc.date{% else %}ds.date{% endif %} < date_trunc('{{ resolution }}', now())
    {% if is_incremental() %}
        AND {% if aggregation_column is not none %}dvc.date{% else %}ds.date{% endif %} > (SELECT max(date) FROM {{ this }})
    {% endif %}
ORDER BY 
    {% if aggregation_column is not none %}dvc.date, dvc.{{ aggregation_column }}{% else %}ds.date{% endif %}

{% endmacro %}