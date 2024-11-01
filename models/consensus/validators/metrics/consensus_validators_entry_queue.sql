{{ 
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='partition_month',
        settings={
            "allow_nullable_key": 1
        }
    ) 
}}

WITH 

{% if is_incremental() %}
last_partition AS (
    SELECT max(partition_month) as partition_month
    FROM {{ this }}
),
{% endif %}

validators_in_activation_queue AS (
    SELECT 
        toDate(f_eth1_block_timestamp) AS day,
        toStartOfMonth(toDate(f_eth1_block_timestamp)) AS partition_month,
        CAST(COUNT(*) AS Int64) AS cnt
    FROM {{ ref('consensus_validators_queue') }}
    {% if is_incremental() %}
    WHERE toStartOfMonth(toDate(f_eth1_block_timestamp)) >= (SELECT partition_month FROM last_partition)
    {% endif %}
    GROUP BY 1, 2
),

validators_activated AS (
    SELECT 
        toDate(activation_time) AS day,
        toStartOfMonth(toDate(activation_time)) AS partition_month,
        CAST(COUNT(*) AS Int64) AS cnt
    FROM {{ ref('consensus_validators_queue') }}
    {% if is_incremental() %}
    WHERE toStartOfMonth(toDate(f_eth1_block_timestamp)) >= (SELECT partition_month FROM last_partition)
    {% endif %}
    GROUP BY 1, 2
),

min_max_dates AS (
    SELECT 
        min(dates.day) as min_date,
        max(dates.day) as max_date
    FROM (
        SELECT day FROM validators_in_activation_queue
        UNION ALL
        SELECT day FROM validators_activated
    ) dates
),

calendar AS (
    SELECT 
        toDate(min_date + number) AS day,
        toStartOfMonth(toDate(min_date + number)) AS partition_month
    FROM min_max_dates
    CROSS JOIN (
        SELECT arrayJoin(range(dateDiff('day', min_date, max_date) + 1)) AS number
        FROM min_max_dates
    ) s
),

daily_metrics AS (
    SELECT
        c.day AS day,
        c.partition_month As partition_month,
        COALESCE(q.cnt, 0) AS validators_entered_queue,
        COALESCE(a.cnt, 0) AS validators_activated,
        COALESCE(q.cnt, 0) - COALESCE(a.cnt, 0) AS net_queue_change
    FROM calendar c
    LEFT JOIN validators_in_activation_queue q ON c.day = q.day
    LEFT JOIN validators_activated a ON c.day = a.day
)


SELECT
    day,
    partition_month,
    validators_entered_queue,
    validators_activated,
    net_queue_change
FROM daily_metrics