{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(visit_ended_at)',
        unique_key='(visit_ended_at,peer_id)',
        partition_by='toStartOfMonth(visit_ended_at)',
        pre_hook=[
        "SET allow_experimental_json_type = 1"
        ]
    )
}}


WITH

fork_digests AS (
    SELECT 
        tupleElement(tup, 1) AS fork_digest,
        tupleElement(tup, 2) AS el_fork_name
    FROM (
        SELECT arrayJoin([
            ('0x56fdb5e0','Istanbul'),
            ('0x824be431','Berlin'),
            ('0x21a6f836','London'),
            ('0x3ebfd484','Merge/Paris'),
            ('0x7d5aab40','Shapella'),
            ('0xf9ab5f85','Dencun')
        ]) AS tup
    )
),

fork_version AS (
    SELECT 
        tupleElement(tup, 1) AS fork_version,
        tupleElement(tup, 2) AS cl_fork_name
    FROM (
        SELECT arrayJoin([
            ('0x00000064','Phase0'),
            ('0x01000064','Altair'),
            ('0x02000064','Bellatrix'),
            ('0x03000064','Capella'),
            ('0x04000064','Deneb'),
            ('0x05000064','Pectra')
        ]) AS tup
    )
),

gnosis_peers AS (
    SELECT 
        visit_ended_at
        ,peer_id
        ,agent_version
        ,CAST(peer_properties.fork_digest AS String) AS fork_digest
        ,t2.el_fork_name 
        ,t3.cl_fork_name 
        ,peer_properties
        ,crawl_error
        ,dial_errors
    FROM 
        {{ source('nebula','visits') }} t1
    LEFT JOIN 
        fork_digests t2
        ON t1.peer_properties.fork_digest = t2.fork_digest
    LEFT JOIN 
        fork_version t3
        ON t1.peer_properties.next_fork_version = t3.fork_version
    WHERE
        (   peer_properties.fork_digest IN (SELECT fork_digest FROM fork_digests) 
            AND
            peer_properties.next_fork_version LIKE '%064'
        )
    {{ apply_monthly_incremental_filter('visit_ended_at',add_and='true') }}
     SETTINGS
        join_use_nulls=1
)

SELECT
    *
FROM gnosis_peers
WHERE visit_ended_at < today()


        
