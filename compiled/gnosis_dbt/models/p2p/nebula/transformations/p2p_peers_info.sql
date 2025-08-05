

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
)


SELECT 
    t1.visit_ended_at,
    t1.peer_id,
    t1.agent_version,
    toString(t1.peer_properties.fork_digest)        AS fork_digest,
    t2.cl_fork_name                                  AS cl_fork_name,
    coalesce(t3.cl_fork_name,
                toString(t1.peer_properties.next_fork_version)) AS cl_next_fork_name,
    t1.peer_properties,
    t1.crawl_error,
    t1.dial_errors
FROM `nebula`.`visits` AS t1
LEFT JOIN fork_digests t2
    ON toString(t1.peer_properties.fork_digest) = t2.fork_digest
LEFT JOIN fork_version t3
    ON toString(t1.peer_properties.next_fork_version) = t3.fork_version
WHERE
    t1.visit_ended_at < today()
    AND
    (
        toString(t1.peer_properties.fork_digest) IN (SELECT fork_digest FROM fork_digests)
        OR
        toString(t1.peer_properties.next_fork_version) LIKE '%064'
    )
    
  
    
      
    

    AND 
    toStartOfMonth(toStartOfDay(visit_ended_at)) >= (
      SELECT
        max(toStartOfMonth(visit_ended_at))
      FROM `dbt`.`p2p_peers_info`
    )
  
