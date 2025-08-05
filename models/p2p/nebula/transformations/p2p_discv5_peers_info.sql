{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(visit_ended_at, peer_id)',
        unique_key='(visit_ended_at, peer_id)',
        partition_by='toStartOfMonth(visit_ended_at)',
        pre_hook=[
          "SET allow_experimental_json_type = 1",
          "SET enable_dynamic_type = 1",
          "SET join_use_nulls = 1"
        ]
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

  peers AS (
    SELECT 
      t1.visit_ended_at,
      t1.peer_id,
      toString(t1.peer_properties.fork_digest)        AS fork_digest,
      t2.cl_fork_name                                 AS cl_fork_name,
      coalesce(
        t3.cl_fork_name,
        toString(t1.peer_properties.next_fork_version)
      )                                               AS cl_next_fork_name,
      t1.agent_version,
      t1.peer_properties,
      t1.crawl_error,
      t1.dial_errors
    FROM {{ source('nebula_discv5','visits') }} AS t1
    LEFT JOIN fork_digests t2
      ON toString(t1.peer_properties.fork_digest) = t2.fork_digest
    LEFT JOIN fork_version t3
      ON toString(t1.peer_properties.next_fork_version) = t3.fork_version
    WHERE
      t1.visit_ended_at < today()
      AND (
        toString(t1.peer_properties.fork_digest) IN (
          SELECT fork_digest FROM fork_digests
        )
        OR toString(t1.peer_properties.next_fork_version) LIKE '%064'
      )
      {{ apply_monthly_incremental_filter('visit_ended_at', add_and='true') }}
  ),

  parsed AS (
    SELECT
      visit_ended_at,
      peer_id,
      fork_digest,
      cl_fork_name,
      cl_next_fork_name,
      peer_properties,
      crawl_error,
      dial_errors,
      agent_version,
      splitByChar('/', agent_version)      AS slash_parts,
      length(slash_parts)                  AS sp_len
    FROM peers
  ),

  with_parts AS (
    SELECT
      visit_ended_at,
      peer_id,
      fork_digest,
      cl_fork_name,
      cl_next_fork_name,
      peer_properties,
      crawl_error,
      dial_errors,
      agent_version,
      slash_parts,
      sp_len,
      -- Extract platform and runtime
      IF(
        sp_len >= 4,
        arrayElement(slash_parts, toInt64(sp_len) - 1),
        arrayElement(slash_parts, sp_len)
      )                                    AS platform,
      IF(
        sp_len >= 4,
        arrayElement(slash_parts, sp_len),
        ''
      )                                    AS runtime,
      -- Head parts for client/variant/ver_blob
      arraySlice(
        slash_parts,
        1,
        IF(
          sp_len >= 4, toInt64(sp_len) - 2,
          toInt64(sp_len) - 1
        )
      )                                    AS head_parts
    FROM parsed
  ),

  exploded AS (
    SELECT
      visit_ended_at,
      peer_id,
      fork_digest,
      cl_fork_name,
      cl_next_fork_name,
      peer_properties,
      crawl_error,
      dial_errors,
      head_parts[1]                       AS client,
      length(head_parts)                  AS hp_len,
      -- Variant logic
      IF(
        hp_len = 3,
        head_parts[2],
        ''
      )                                    AS variant,
      -- Raw version blob
      head_parts[hp_len]                  AS ver_blob,
      platform,
      runtime,
      -- Split + metadata
      IF(
        ver_blob LIKE '%+%',
        arrayElement(splitByChar('+', ver_blob), 1),
        ver_blob
      )                                    AS pre_blob,
      IF(
        ver_blob LIKE '%+%',
        arrayElement(splitByChar('+', ver_blob), 2),
        ''
      )                                    AS plus_build
    FROM with_parts
  )

SELECT
  visit_ended_at,
  peer_id,
  fork_digest,
  cl_fork_name,
  cl_next_fork_name,
  peer_properties,
  crawl_error,
  dial_errors,
  client,
  variant,
  -- Final version
  IF(
    plus_build != '',
    arrayElement(splitByChar('-', pre_blob), 1),
    splitByChar('-', ver_blob)[1]
  )                                    AS version,
  -- Channel
  IF(
    plus_build != '',
    IF(
      length(splitByChar('-', pre_blob)) >= 2,
      arrayElement(splitByChar('-', pre_blob), 2),
      ''
    ),
    IF(
      length(splitByChar('-', ver_blob)) = 3,
      arrayElement(splitByChar('-', ver_blob), 2),
      ''
    )
  )                                    AS channel,
  -- Build metadata
  IF(
    plus_build != '',
    plus_build,
    IF(
      length(splitByChar('-', ver_blob)) > 1,
      arrayElement(splitByChar('-', ver_blob), -1),
      ''
    )
  )                                    AS build,
  platform,
  runtime
FROM exploded