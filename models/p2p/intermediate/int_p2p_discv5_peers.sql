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

-- Known fork digests → names
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

-- Known fork versions → names
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

/* Pull only relevant rows from source and normalize Dynamic→String once */
peers AS (
  SELECT 
    t1.crawl_id,
    t1.visit_ended_at,
    t1.peer_id,
    t1.connect_maddr,
    -- Dynamic JSON leaves → String for safe joins/filters
    toString(t1.peer_properties.fork_digest)         AS fork_digest,
    toString(t1.peer_properties.next_fork_version)   AS next_fork_version,

    -- Map to fork names
    t2.cl_fork_name                                  AS cl_fork_name,
    coalesce(t3.cl_fork_name, toString(t1.peer_properties.next_fork_version))
                                                    AS cl_next_fork_name,

    t1.agent_version,
    t1.peer_properties,
    t1.crawl_error,
    t1.dial_errors
  FROM {{ ref('stg_nebula_discv5__visits') }} AS t1
  LEFT JOIN fork_digests t2
    ON toString(t1.peer_properties.fork_digest) = t2.fork_digest
  LEFT JOIN fork_version t3
    ON toString(t1.peer_properties.next_fork_version) = t3.fork_version
  WHERE
    (
      toString(t1.peer_properties.fork_digest) IN (SELECT fork_digest FROM fork_digests)
      OR toString(t1.peer_properties.next_fork_version) LIKE '%064'
    )
    {{ apply_monthly_incremental_filter('visit_ended_at', add_and='true') }}
),

/* Split and locate version token via regex */
parsed AS (
  SELECT
    crawl_id,
    visit_ended_at,
    peer_id,
    connect_maddr,
    fork_digest,
    next_fork_version,
    cl_fork_name,
    cl_next_fork_name,
    peer_properties,
    crawl_error,
    dial_errors,
    agent_version,

    splitByChar('/', agent_version)                                         AS parts,
    length(splitByChar('/', agent_version))                                 AS parts_len,
    arraySlice(splitByChar('/', agent_version), 2)                         AS tail,
    length(arraySlice(splitByChar('/', agent_version), 2))                 AS tail_len,

    splitByChar('/', agent_version)[1]                                     AS client,

    -- first tail index that looks like a version (v?digits(.digits){0,3}…)
    arrayFirstIndex(x ->
        (substring(x, 1, 1) = 'v' OR match(x, '^[0-9]')) AND
        match(x, '^v?[0-9]+(\\.[0-9]+){0,3}([\\-\\w\\.\\+]+)?$')
      , arraySlice(splitByChar('/', agent_version), 2))                    AS ver_idx_tail
  FROM peers
),

/* Derive variant, version blob, platform, runtime */
with_parts AS (
  SELECT
    crawl_id,
    visit_ended_at,
    peer_id,
    connect_maddr,
    fork_digest,
    next_fork_version,
    cl_fork_name,
    cl_next_fork_name,
    peer_properties,
    crawl_error,
    dial_errors,
    agent_version,
    parts,
    parts_len,
    tail,
    tail_len,
    client,
    ver_idx_tail,

    /* variant: exactly one token between client and version */
    IF(ver_idx_tail > 1, tail[1], '')                                       AS variant,

    /* raw version token (may include '-' channel and/or '+' build) */
    IF(ver_idx_tail > 0, tail[ver_idx_tail], '')                            AS ver_blob,

    /* tokens after version for platform/runtime */
    IF(ver_idx_tail > 0 AND tail_len >= ver_idx_tail + 1, tail[ver_idx_tail + 1], '')
      AS platform,
    IF(ver_idx_tail > 0 AND tail_len >= ver_idx_tail + 2, tail[ver_idx_tail + 2], '')
      AS runtime
  FROM parsed
),

/* Split version blob and prep hyphen parts; also clean runtime */
exploded AS (
  SELECT
    crawl_id,
    visit_ended_at,
    peer_id,
    agent_version,
    connect_maddr,
    fork_digest,
    next_fork_version,
    cl_fork_name,
    cl_next_fork_name,
    peer_properties,
    crawl_error,
    dial_errors,
    client,
    variant,
    ver_blob,

    -- strip leading '-' in runtime
    replaceRegexpOne(runtime, '^-+', '')                                    AS runtime,
    platform,

    /* version blob split around '+' */
    IF(position(ver_blob, '+') > 0, splitByChar('+', ver_blob)[1], ver_blob) AS pre_blob,
    IF(position(ver_blob, '+') > 0, splitByChar('+', ver_blob)[2], '')       AS plus_build,

    /* hyphen parts for channel/build logic (work off pre_blob) */
    splitByChar('-', IF(position(ver_blob, '+') > 0, splitByChar('+', ver_blob)[1], ver_blob))
                                                                            AS hy_parts
  FROM with_parts
),

basic_info AS (
  SELECT
    crawl_id,
    visit_ended_at,
    peer_id,
    agent_version,
    replaceRegexpAll(connect_maddr, '^/ip4/([0-9.]+)/tcp/[0-9]+$', '\\1') AS ip,
    fork_digest,
    cl_fork_name,
    cl_next_fork_name,
    next_fork_version,
    peer_properties,
    crawl_error,
    dial_errors,
    client,
    variant,
    IF(length(hy_parts) >= 1, hy_parts[1], '')                                AS version,
    IF(length(hy_parts) >= 3, hy_parts[2], '')                                 AS channel,
    IF(
      plus_build != '',
      plus_build,
      IF(length(hy_parts) >= 2, hy_parts[length(hy_parts)], '')
    )                                                                          AS build,
    platform,
    runtime
  FROM exploded
)

SELECT
  t1.crawl_id,
  t1.visit_ended_at,
  t1.peer_id,
  t1.agent_version,
  t1.ip,
  t1.fork_digest,
  t1.cl_fork_name,
  t1.cl_next_fork_name,
  t1.next_fork_version,
  t1.peer_properties,
  t1.crawl_error,
  t1.dial_errors,
  t1.client,
  t1.variant,
  t1.version,
  t1.channel,
  t1.build,
  CASE
      WHEN t1.platform = '' THEN 'Unknown'
      WHEN t1.platform = 'aarch64-linux' THEN 'linux-aarch_64'
      WHEN t1.platform = 'x86_64-linux' THEN 'linux-x86_64'
      WHEN t1.platform = 'x86_64-windows' THEN 'windows-x86_64'
      ELSE t1.platform
  END AS platform,
  t1.runtime,
  t2.hostname   AS peer_hostname,
  t2.city       AS peer_city,
  t2.country    AS peer_country,
  t2.org        AS peer_org,
  t2.loc        AS peer_loc,
  t2.generic_provider AS generic_provider
FROM
  basic_info t1
LEFT JOIN {{ ref('stg_crawlers_data__ipinfo') }} AS t2
  ON t2.ip = t1.ip