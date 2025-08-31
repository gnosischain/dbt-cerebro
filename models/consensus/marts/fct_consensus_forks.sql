{{
    config(
        materialized='view',
        tags=["production", "consensus", "forks"]
    )
}}

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
        IF(fork_name='GENESIS', 'PHASE0', fork_name) AS fork_name
        ,parameter_value
    FROM (
        SELECT
            arrayElement(splitByChar('_', ifNull(parameter_name, '')), 1) AS fork_name
            ,parameter_value
        FROM {{ ref('stg_consensus__specs') }}
        WHERE parameter_name LIKE '%_FORK_VERSION'
    )
),

fork_epoch AS (
    SELECT
        arrayElement(splitByChar('_', ifNull(parameter_name, '')), 1) AS fork_name
        ,parameter_value
    FROM {{ ref('stg_consensus__specs') }}
    WHERE parameter_name LIKE '%_FORK_EPOCH'

    UNION ALL 

    SELECT 'PHASE0' AS fork_name, '0' AS parameter_value 
)

SELECT 
  t1.cl_fork_name AS fork_name
  ,t2.parameter_value AS fork_version 
  ,t1.fork_digest AS fork_digest
  ,t3.parameter_value AS fork_epoch 
  ,IF(CAST(t3.parameter_value AS Int) = -1, NULL,
    addSeconds(
          toDateTime(t4.genesis_time_unix, 'UTC'),
          CAST(t3.parameter_value AS Int) * (t4.seconds_per_slot * t4.slots_per_epoch )
      )
   ) AS fork_time
FROM 
  fork_digests t1
INNER JOIN
  fork_version t2
  ON LOWER(t2.fork_name) = LOWER(t1.cl_fork_name)
INNER JOIN
  fork_epoch t3
  ON LOWER(t3.fork_name) = LOWER(t1.cl_fork_name)
CROSS JOIN {{ ref('stg_consensus__time_helpers') }} t4