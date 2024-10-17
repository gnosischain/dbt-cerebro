{{ config(
    materialized='table',
    unique_key='enr',
) }}

SELECT DISTINCT
    t1.enr
    ,t2.city
    ,t2.country
    ,t2.latitude
    ,t2.longitude
    ,t2.asn_organization
    ,t2.asn_type
FROM 
    {{ source('valtrack','metadata_received_events') }} t1
INNER JOIN
    {{ source('valtrack','peer_discovered_events') }} t3 
    ON
    t3.enr = t1.enr
INNER JOIN
    {{ source('valtrack','ip_metadata') }} t2
    ON t2.ip = t3.ip
WHERE
    t1.timestamp >= date_trunc('day', now() - INTERVAL 5 DAY)
    AND
    t1.timestamp < date_trunc('day', now()- INTERVAL 4 DAY)