


WITH

fork_digests AS (
    SELECT 
        tupleElement(tup, 1) AS fork_digest,
        tupleElement(tup, 2) AS cl_fork_name
    FROM (
        SELECT arrayJoin([
            ('0xbc9a6864','Phase0'),
            ('0x56fdb5e0','Altair'),
            ('0x824be431','Bellatrix'),
            ('0x21a6f836','Capella'),
            ('0x3ebfd484','Deneb'),
            ('0x7d5aab40','Electra'),
            ('0xf9ab5f85','Fulu')
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
            ('0x05000064','Electra'),
            ('0x06000064','Fulu')
        ]) AS tup
    )
),

gnosis_peers AS (
    SELECT 
        visit_ended_at
        ,peer_id
        ,agent_version
        ,CAST(peer_properties.fork_digest AS String) AS fork_digest
        ,t2.cl_fork_name AS cl_fork_name
        ,COALESCE(t3.cl_fork_name,peer_properties.next_fork_version) AS cl_next_fork_name
        ,peer_properties
        ,crawl_error
        ,dial_errors
    FROM 
        `nebula`.`visits` t1
    LEFT JOIN 
        fork_digests t2
        ON t1.peer_properties.fork_digest = t2.fork_digest
    LEFT JOIN 
        fork_version t3
        ON t1.peer_properties.next_fork_version = t3.fork_version
    WHERE
        (   peer_properties.fork_digest IN (SELECT fork_digest FROM fork_digests) 
            OR
            peer_properties.next_fork_version LIKE '%064'
        )
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(visit_ended_at))
      FROM `dbt`.`p2p_peers_info`
    )
  

     SETTINGS
        join_use_nulls=1
)

SELECT
    *
FROM gnosis_peers
WHERE visit_ended_at < today()