

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

visits_info AS (
    SELECT
        toStartOfDay(visit_ended_at) AS date
        ,COUNT(visit_ended_at) AS total_visits
        ,SUM(IF( empty(dial_errors) = 1 OR crawl_error IS NULL, 1, 0)) AS successful_visits
        ,COUNT(DISTINCT crawl_id) AS crawls
    FROM `dbt`.`stg_nebula_discv5__visits`
    WHERE
      toStartOfDay(visit_ended_at) < today()
      AND
      (
        toString(peer_properties.fork_digest) IN (SELECT fork_digest FROM fork_digests)
        OR toString(peer_properties.next_fork_version) LIKE '%064'
      )
      
  
    
    

   AND 
    toStartOfMonth(toDate(visit_ended_at)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_p2p_discv5_visits_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(visit_ended_at) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_p2p_discv5_visits_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
)

SELECT * FROM visits_info