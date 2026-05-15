

-- Single scan of int_execution_safes_owner_events.
-- Owner-grain rows (with owner) and safe-grain threshold rows (no owner) are
-- aggregated in one GROUP BY (safe_address, owner) pass; threshold rows
-- naturally fall into their own bucket because owner IS NULL there.
-- We then compute current_threshold per safe_address as a window over the
-- aggregated rows — far smaller than re-scanning the source.
WITH base AS (
    SELECT
        safe_address,
        owner,
        event_kind,
        threshold,
        block_number,
        block_timestamp,
        log_index
    FROM `dbt`.`int_execution_safes_owner_events`
    WHERE event_kind IN ('safe_setup','added_owner','removed_owner','changed_threshold')
),

agg AS (
    SELECT
        safe_address,
        owner,
        argMax(event_kind,      (block_number, log_index)) AS last_event_kind,
        argMax(block_timestamp, (block_number, log_index)) AS last_event_time,
        argMaxIf(threshold,     (block_number, log_index),
                 event_kind IN ('safe_setup','changed_threshold') AND threshold IS NOT NULL)
                                                            AS last_threshold_here,
        max((block_number, log_index))                       AS last_pos
    FROM base
    GROUP BY safe_address, owner
),

threshold_per_safe AS (
    SELECT
        safe_address,
        argMax(last_threshold_here, last_pos) AS latest_threshold
    FROM agg
    WHERE last_threshold_here IS NOT NULL
    GROUP BY safe_address
)

SELECT
    a.safe_address,
    a.owner,
    a.last_event_time           AS became_owner_at,
    t.latest_threshold          AS current_threshold
FROM agg a
LEFT JOIN threshold_per_safe t USING (safe_address)
WHERE a.owner IS NOT NULL
  AND a.last_event_kind IN ('safe_setup','added_owner')