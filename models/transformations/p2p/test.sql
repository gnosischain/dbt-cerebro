-- models/gnosis/gnosis_enr_ranked_records.sql

{{ 
    config(
            materialized='table'
        ) 
}}

WITH gnosis_enr AS (
    SELECT
        t1.enr,
        to_hex(t2.fork_digest) AS fork_digest,
        to_hex(t2.next_fork_version) AS next_fork_version,
        t2.next_fork_epoch
    FROM 
        {{ source('gnosis_xatu', 'node_record') }} t1,
        LATERAL (SELECT {{ deserialize_eth2('t1.eth2') }}) AS t2 
    WHERE 
        to_hex(t2.next_fork_version) IN ('01000064', '02000064', '03000064', '04000064', '05000064')
),

RankedRecords AS (
    SELECT 
        t1.*
        --ROW_NUMBER() OVER (PARTITION BY t1.ip4 ORDER BY t1.create_time DESC) AS rn,
        --ROW_NUMBER() OVER (PARTITION BY t1.node_id ORDER BY t1.create_time DESC) AS rn2
    FROM 
        {{ source('gnosis_xatu', 'node_record') }} t1
    INNER JOIN 
        gnosis_enr t2 ON t2.enr = t1.enr
)

SELECT 
	CAST(create_time AS timestamp) AS create_time
	,CAST(last_dial_time AS timestamp) AS last_dial_time
	,consecutive_dial_attempts
	,CAST(last_connect_time AS timestamp) AS last_connect_time
	,to_hex(signature) AS signature
	,geo_longitude
	,geo_latitude
	,geo_autonomous_system_number
	,secp256k1
	--,ip4
	--,ip6
	,tcp4
	,udp4
	,tcp6
	,udp6
	,eth2
	,attnets
	,syncnets
	,seq
	,geo_autonomous_system_organization
	,id
	,node_id
	,peer_id
	,geo_city
	,geo_country
	,geo_country_code
	,geo_continent_code
	,enr
FROM 
    RankedRecords
--WHERE 
--    rn = 1
    -- Additional conditions can be uncommented as needed
    -- AND rn2 = 1
    -- AND ip4 = '46.162.82.129'
