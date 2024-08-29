{% macro node_count_over_time(group_by_column, source_model, grouping_column_name='grouping_column', res = 'hour') %}

{% set current_timestamp %}
    SELECT DATE_TRUNC('hour', MAX(last_seen)) AS current_hour FROM {{ source_model }}
{% endset %}


WITH NodeStatus AS (
    SELECT
        node_id,
        last_seen,
        last_seen_lead,
        CASE
            WHEN {{ group_by_column }}  IS NULL THEN 'UNKNOWN'
            ELSE {{ group_by_column }} 
        END AS {{ grouping_column_name }},
        status
    FROM {{ source_model }}
    WHERE last_seen < ({{ current_timestamp }})
),


active AS (
    SELECT
        datetime
		,{{ grouping_column_name }}
		,COUNT(*) AS cnt
    FROM (
        SELECT  DISTINCT
            node_id
            ,{{ grouping_column_name }}
            ,GENERATE_SERIES(DATE_TRUNC('{{res}}', last_seen), DATE_TRUNC('{{res}}', last_seen_lead), '1 {{res}}'::INTERVAL) AS datetime
        FROM NodeStatus
        WHERE status = 'active'
    ) t
    GROUP BY 1, 2
),

calendar AS (
    SELECT 
        t1.datetime,
        t2.{{ grouping_column_name }}
    FROM (
        SELECT generate_series(
            (SELECT MIN(datetime) FROM active),
            ({{ current_timestamp }}),
            '1 {{res}}'::interval 
        ) AS datetime
    ) t1
    CROSS JOIN (SELECT DISTINCT {{ grouping_column_name }} FROM active) t2
),

final AS (
    SELECT 
		t1.datetime
		,t1.{{ grouping_column_name }}
		,t2.cnt
	FROM calendar t1
    LEFT JOIN active t2
        ON t1.datetime = t2.datetime 
        AND t1.{{ grouping_column_name }} = t2.{{ grouping_column_name }} 
)

SELECT * FROM final
WHERE cnt IS NOT NULL
{% if is_incremental() %}
AND datetime > (SELECT MAX(datetime) FROM {{ this }})
{% endif %}

{% endmacro %}