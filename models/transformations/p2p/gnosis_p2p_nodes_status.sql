{{ config(materialized='incremental', unique_key='last_seen') }}

{% set activity_buffer = '72 hours' %}
{% set chain_code = '00064' %}

WITH 

gnosis_nodes AS (
	SELECT
		enr
		,node_id
        ,create_time AS last_seen
		,COALESCE(LEAD(create_time) OVER (PARTITION BY enr ORDER BY create_time), CURRENT_TIMESTAMP) AS last_seen_lead
		,geo_longitude
		,geo_latitude
		,geo_autonomous_system_organization
		,geo_city
		,geo_country
		,geo_country_code
		,geo_continent_code
		,fork_digest
		,next_fork_version
		,CASE
			WHEN next_fork_version = CONCAT('010','{{ chain_code }}') THEN 'ALTAIR'
			WHEN next_fork_version = CONCAT('020','{{ chain_code }}') THEN 'BELLATRIX'
			WHEN next_fork_version = CONCAT('030','{{ chain_code }}') THEN 'CAPELLA'
			WHEN next_fork_version = CONCAT('040','{{ chain_code }}') THEN 'DENEB'
			WHEN next_fork_version = CONCAT('050','{{ chain_code }}') THEN 'ELECTRA'
		END next_fork_label
		,next_fork_epoch
		,CASE 
                WHEN LOWER(geo_autonomous_system_organization) ~* 'amazon' THEN 'Amazon'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'google' THEN 'Google'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'microsoft' THEN 'Microsoft'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'oracle' THEN 'Oracle'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'alibaba' THEN 'Alibaba'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'huawei clouds' THEN 'Huawei'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'hetzner' THEN 'Hetzner'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'contabo' THEN 'Contabo'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'ovh sas' THEN 'OVH SAS'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'netcup' THEN 'Netcup'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'limestone' THEN 'Limestone'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'allnodes' THEN 'Allnodes'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'teraswitch' THEN 'Teraswitch'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'latitude-sh' THEN 'Latitude-sh'
                WHEN LOWER(geo_autonomous_system_organization) ~* 'datacamp' THEN 'Datacamp'
                ELSE 'Self Hosted'
        END AS provider
	FROM (
		SELECT 
			t1.*,
			t2.fork_digest,
			t2.next_fork_version,
			t2.next_fork_epoch
		FROM 
			{{ source('gnosis_xatu','node_record') }} t1, 
			LATERAL deserialize_eth2(t1.eth2) t2
	) t
	WHERE 
		next_fork_version LIKE CONCAT('%','{{ chain_code }}')

),

NodeStatus AS (
    SELECT
        *
		,LEAST(last_seen + INTERVAL '{{ activity_buffer }}', last_seen_lead) AS active_until
    FROM
        gnosis_nodes
),

SplitStatus AS (
	SELECT
		enr,
        node_id,
        last_seen,
        active_until AS last_seen_lead,
        'active' AS status,
        geo_longitude,
        geo_latitude,
        geo_autonomous_system_organization,
        geo_city,
        geo_country,
        geo_country_code,
        geo_continent_code,
        fork_digest,
        next_fork_version,
        next_fork_label,
        next_fork_epoch,
        provider
    FROM NodeStatus
    
    UNION ALL
    
    SELECT
		enr,
        node_id,
        active_until AS last_seen,
        last_seen_lead,
        'inactive' AS status,
        geo_longitude,
        geo_latitude,
        geo_autonomous_system_organization,
        geo_city,
        geo_country,
        geo_country_code,
        geo_continent_code,
        fork_digest,
        next_fork_version,
        next_fork_label,
        next_fork_epoch,
        provider
    FROM NodeStatus
    WHERE active_until < last_seen_lead
)

SELECT * FROM SplitStatus