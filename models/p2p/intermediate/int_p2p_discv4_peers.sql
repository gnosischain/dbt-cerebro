{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(visit_ended_at, peer_id)',
        unique_key='(visit_ended_at, peer_id)',
        partition_by='toStartOfMonth(visit_ended_at)',
        pre_hook=[
          "SET allow_experimental_json_type = 1"
        ]
    ) 
}}

WITH

  peers AS (
    SELECT 
      visit_ended_at,
      peer_id,
      agent_version,
      connect_maddr,
      peer_properties,
      crawl_error,
      dial_errors
  FROM {{ ref('stg_nebula_discv4__visits') }} A
  WHERE
      toString(peer_properties.network_id) = '100'
      {{ apply_monthly_incremental_filter('visit_ended_at', add_and='true') }}
  ),

  parsed AS (
    SELECT
      visit_ended_at,
      peer_id,
      agent_version,
      connect_maddr,
      peer_properties,
      crawl_error,
      dial_errors,
      -- break into slash-delimited parts
      splitByChar('/', agent_version)                        AS slash_parts,
      length(slash_parts)                                    AS sp_len,

      -- if 4+ parts, take last two as platform+runtime; otherwise only platform
      IF(
        length(slash_parts) > 3,
        arrayElement(slash_parts, sp_len-1),
        arrayElement(slash_parts, sp_len)
      )                                                       AS platform,

      IF(
        length(slash_parts) > 3,
        arrayElement(slash_parts, sp_len),
        ''
      )                                                       AS runtime,

      -- head_parts = everything before the last 1 or 2 elements
      arraySlice(
        slash_parts,
        1,
        sp_len - IF(sp_len > 3, 2, 1)
      )                                                       AS head_parts

    FROM peers
  ),

  exploded AS (
    SELECT
      visit_ended_at,
      peer_id,
      agent_version,
      connect_maddr,
      peer_properties,
      crawl_error,
      dial_errors,
      head_parts[1]                                          AS client,

      -- if head_parts has 3 elements, the middle is variant
      IF(
        length(head_parts) = 3,
        head_parts[2],
        ''
      )                                                       AS variant,

      -- the last element of head_parts is our raw “ver_blob”
      arrayElement(head_parts, length(head_parts))            AS ver_blob,

      platform,
      runtime,

      -- split out “+”-style metadata
      IF(ver_blob LIKE '%+%', arrayElement(splitByChar('+', ver_blob), 1), ver_blob) AS pre_blob,
      IF(ver_blob LIKE '%+%', arrayElement(splitByChar('+', ver_blob), 2), '')        AS plus_build

    FROM parsed
  ),

basic_info AS (
  SELECT
    visit_ended_at,
    peer_id,
    agent_version,
    --replaceRegexpAll(connect_maddr, '^/ip4/([0-9.]+)/tcp/[0-9]+$', '\\1') AS ip,
    arrayElement(splitByChar('/', ifNull(connect_maddr, '')), 3) AS ip,
    peer_properties,
    crawl_error,
    dial_errors,
    client,
    variant,
    IF(
      plus_build != '',
      arrayElement(splitByChar('-', pre_blob), 1),
      splitByChar('-', ver_blob)[1]
    )                                                       AS version,
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
    )                                                       AS channel,
    IF(
      plus_build != '',
      plus_build,
      IF(
        length(splitByChar('-', ver_blob)) > 1,
        arrayElement(splitByChar('-', ver_blob), -1),
        ''
      )
    )                                                       AS build,
    platform,
    runtime
  FROM exploded
)

SELECT
  t1.visit_ended_at,
  t1.peer_id,
  t1.agent_version,
  t1.ip,
  t1.peer_properties,
  t1.crawl_error,
  t1.dial_errors,
  t1.client,
  t1.variant,
  t1.version,
  t1.channel,
  t1.build,
  t1.platform,
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