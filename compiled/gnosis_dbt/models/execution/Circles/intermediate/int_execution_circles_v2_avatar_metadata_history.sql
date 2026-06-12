
-- SCD-style historical view of every Circles v2 avatar metadata change.
--
-- One row per (avatar, metadata_digest) ever announced by the
-- NameRegistry, in event order, with valid_from / valid_to / is_current
-- columns so the dashboard can render the full timeline of name + image
-- changes for a selected avatar.
--
-- Incremental strategy: delete+insert keyed on (avatar, metadata_digest).
-- On an incremental run we only emit the rows that actually need to
-- change:
--   * new_events     — events whose block_timestamp falls in the
--                      incremental window (these become brand-new rows).
--   * prior_current  — the row currently flagged is_current for any
--                      avatar that shows up in new_events. Its valid_to
--                      flips from NULL to the next event's timestamp and
--                      is_current flips to false. (Skipped when the
--                      prior current digest is itself in new_events.)
-- next_block_timestamp is computed by a windowed lead over the targets
-- view restricted to affected avatars — internal scan only, the
-- per-avatar full history is NOT re-emitted to the table.
--
-- valid_to is NULL on the most recent row per avatar.




WITH new_events AS (
    SELECT
        t.avatar,
        t.metadata_digest,
        t.ipfs_cid_v0,
        t.gateway_url,
        t.block_timestamp,
        t.transaction_hash,
        t.log_index,
        t.is_current_avatar_metadata
    FROM `dbt`.`int_execution_circles_v2_avatar_metadata_targets` t
    WHERE 1 = 1
      
        
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.valid_from)), -0))
        FROM `dbt`.`int_execution_circles_v2_avatar_metadata_history` AS x1
        WHERE 1=1 
      )
      AND toDate(t.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.valid_from)), -0)
          

        FROM `dbt`.`int_execution_circles_v2_avatar_metadata_history` AS x2
        WHERE 1=1 
      )
    
  

      
),

prior_current AS (
    SELECT
        h.avatar,
        h.metadata_digest,
        h.ipfs_cid_v0,
        h.gateway_url,
        h.valid_from                                      AS block_timestamp,
        h.transaction_hash,
        h.log_index,
        false                                             AS is_current_avatar_metadata
    FROM `dbt`.`int_execution_circles_v2_avatar_metadata_history` h
    WHERE h.is_current = true
      AND h.avatar IN (SELECT DISTINCT avatar FROM new_events)
      AND (h.avatar, h.metadata_digest) NOT IN (
          SELECT avatar, metadata_digest FROM new_events
      )
),
touched AS (
    SELECT * FROM new_events
    UNION ALL
    SELECT * FROM prior_current
),

targets_with_lead AS (
    SELECT
        t.avatar,
        t.block_timestamp,
        t.log_index,
        leadInFrame(t.block_timestamp) OVER (
            PARTITION BY t.avatar
            ORDER BY t.block_timestamp, t.log_index
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS next_block_timestamp
    FROM `dbt`.`int_execution_circles_v2_avatar_metadata_targets` t
    WHERE t.avatar IN (SELECT DISTINCT avatar FROM touched)
),
ordered AS (
    SELECT
        e.avatar,
        e.metadata_digest,
        e.ipfs_cid_v0,
        e.gateway_url,
        e.block_timestamp,
        e.transaction_hash,
        e.log_index,
        e.is_current_avatar_metadata,
        twl.next_block_timestamp                        AS next_block_timestamp
    FROM touched e
    LEFT JOIN targets_with_lead twl
        ON twl.avatar = e.avatar
       AND twl.block_timestamp = e.block_timestamp
       AND twl.log_index = e.log_index
),
latest_raw AS (
    SELECT
        avatar,
        metadata_digest,
        http_status,
        content_type,
        body,
        fetched_at,
        row_number() OVER (
            PARTITION BY avatar, metadata_digest
            ORDER BY fetched_at DESC
        ) AS rn
    FROM `dbt`.`circles_avatar_metadata_raw`
    WHERE http_status = 200
      AND body != ''
)
SELECT
    a.avatar                                            AS avatar,
    a.avatar_type                                       AS avatar_type,
    a.name                                              AS onchain_name,
    o.metadata_digest                                   AS metadata_digest,
    o.ipfs_cid_v0                                       AS ipfs_cid_v0,
    o.gateway_url                                       AS gateway_url,
    o.block_timestamp                                   AS valid_from,
    if(o.next_block_timestamp = toDateTime64('1970-01-01 00:00:00', 0, 'UTC'),
       NULL, o.next_block_timestamp)                    AS valid_to,
    o.is_current_avatar_metadata                        AS is_current,
    o.transaction_hash                                  AS transaction_hash,
    o.log_index                                         AS log_index,
    JSONExtractString(r.body, 'name')                   AS metadata_name,
    JSONExtractString(r.body, 'symbol')                 AS metadata_symbol,
    JSONExtractString(r.body, 'description')            AS metadata_description,
    JSONExtractString(r.body, 'imageUrl')               AS metadata_image_url,
    JSONExtractString(r.body, 'previewImageUrl')        AS metadata_preview_image_url,
    r.body                                              AS metadata_body,
    r.fetched_at                                        AS metadata_fetched_at
FROM ordered o
INNER JOIN `dbt`.`int_execution_circles_v2_avatars` a
    ON a.avatar = o.avatar
LEFT JOIN latest_raw r
    ON o.avatar = r.avatar
   AND o.metadata_digest = r.metadata_digest
   AND r.rn = 1