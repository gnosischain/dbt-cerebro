-- models/gnosis/gnosis_enr_ranked_records.sql
{{ 
    config(
            materialized='table'
        ) 
}}

    SELECT 
        t1.enr
        ,t1.peer_id
        ,t1.geo_city
    FROM 
        {{ source('gnosis_xatu', 'node_record') }} AS t1

