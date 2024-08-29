{{ config(materialized='incremental', unique_key='datetime') }}

WITH 


NodeStatus AS (
    SELECT
        node_id
		,last_seen
		,last_seen_lead
		,status
    FROM
        {{ ref('gnosis_p2p_nodes_status') }}
),


first_time AS (
	SELECT
		node_id
		,MIN(DATE_TRUNC('hour', last_seen)) as datetime
	FROM
		NodeStatus
	GROUP BY
		1
),

allnodes AS (

	SELECT
		datetime
		,COUNT(*) AS cnt
	FROM
		first_time
	GROUP BY 1

),

inactive AS (
	SELECT
		DATE_TRUNC('hour', last_seen_lead) as datetime
		,COUNT(DISTINCT node_id) AS cnt
	FROM
		NodeStatus
	WHERE
		status = 'inactive'
	GROUP BY
		1
)


SELECT 
	t1.datetime
	,(SUM(t1.cnt) OVER (ORDER BY t1.datetime)) - COALESCE(t2.cnt,0) AS cnt
FROM 
	allnodes t1
LEFT JOIN
	inactive t2
	ON 
	t2.datetime = t1.datetime
ORDER BY
	1 ASC 