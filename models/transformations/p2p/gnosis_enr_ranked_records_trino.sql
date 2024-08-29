-- models/gnosis/gnosis_enr_ranked_records.sql
{{ config(materialized='table') }}

    SELECT 
        t1.*
    FROM 
        {{ source('gnosis_xatu', 'node_record') }} AS t1

