{% macro node_count_over_time_old(group_by_column, source_model, grouping_column_name='grouping_column') %}

{% set current_timestamp %}
    SELECT DATE_TRUNC('hour', MAX(last_seen)) AS current_hour FROM {{ source_model }}
{% endset %}


WITH NodeStatus AS (
    SELECT
        node_id,
        last_seen,
        last_seen_lead,
       {{ group_by_column }} AS {{ grouping_column_name }},
        status
    FROM {{ source_model }}
    WHERE last_seen < ({{ current_timestamp }})
),

first_time AS (
    SELECT
        node_id,
        {{ group_by_column }} AS {{ grouping_column_name }},
        MIN(DATE_TRUNC('hour', last_seen)) AS datetime
    FROM {{ source_model }}
    WHERE 
        last_seen < ({{ current_timestamp }})
    GROUP BY node_id, {{ grouping_column_name }}
),

allnodes AS (
    SELECT
        datetime,
        {{ grouping_column_name }},
        COUNT(*) AS cnt
    FROM first_time
    GROUP BY 1, 2
),


hourly_data AS (
    SELECT
        DATE_TRUNC('hour', last_seen) as datetime,
        {{ grouping_column_name }},
        node_id
    FROM NodeStatus
    WHERE status = 'inactive'
),
ranked_data AS (
    SELECT
        datetime,
        {{ grouping_column_name }},
        node_id,
        ROW_NUMBER() OVER (PARTITION BY {{ grouping_column_name }}, node_id ORDER BY datetime) as rn
    FROM hourly_data
),
first_appearance AS (
    SELECT
        datetime,
        {{ grouping_column_name }},
        node_id
    FROM ranked_data
    WHERE rn = 1
),



inactive AS (
SELECT
    datetime,
    {{ grouping_column_name }},
    cnt
FROM (
    SELECT
    datetime,
    {{ grouping_column_name }},
    COUNT(*) OVER (PARTITION BY {{ grouping_column_name }} ORDER BY datetime) as cnt,
    ROW_NUMBER(*) OVER (PARTITION BY {{ grouping_column_name }} ORDER BY datetime) as rn
FROM first_appearance
) t
WHERE rn = 1
    
),


max_date AS (
    SELECT MAX(datetime) as max_datetime FROM allnodes
),

calendar AS (
    SELECT 
        t1.datetime,
        t2.{{ grouping_column_name }}
    FROM (
        SELECT generate_series(
            (SELECT MIN(datetime) FROM allnodes),
            ({{ current_timestamp }}),
            '1 hour'::interval 
        ) AS datetime
    ) t1
    CROSS JOIN (SELECT DISTINCT {{ grouping_column_name }} FROM allnodes) t2
),

nodes_cumulative AS (
    SELECT 
        t1.datetime,
        t1.{{ grouping_column_name }},
        SUM(COALESCE(t2.cnt,0)) OVER (PARTITION BY t1.{{ grouping_column_name }} ORDER BY t1.datetime) AS cnt
    FROM calendar t1
    LEFT JOIN allnodes t2
        ON t2.datetime = t1.datetime
        AND t2.{{ grouping_column_name }} = t1.{{ grouping_column_name }}
),


final AS (
    SELECT 
        t1.datetime,
        t1.{{ grouping_column_name }},
        t2.cnt - COALESCE(t3.cnt,0) AS cnt
    FROM calendar t1
    INNER JOIN nodes_cumulative t2
        ON t1.datetime = t2.datetime
        AND t1.{{ grouping_column_name }} = t2.{{ grouping_column_name }}
    LEFT JOIN inactive t3
        ON t3.datetime = t1.datetime
        AND t3.{{ grouping_column_name }} = t1.{{ grouping_column_name }}
)

SELECT * FROM final
WHERE cnt IS NOT NULL
{% if is_incremental() %}
AND datetime > (SELECT MAX(datetime) FROM {{ this }})
{% endif %}

{% endmacro %}