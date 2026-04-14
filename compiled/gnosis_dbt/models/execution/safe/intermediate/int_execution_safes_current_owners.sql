



WITH owner_latest AS (
    SELECT
        safe_address,
        owner,
        argMax(event_kind,      (block_number, log_index)) AS last_event_kind,
        argMax(block_timestamp, (block_number, log_index)) AS last_event_time
    FROM `dbt`.`int_execution_safes_owner_events`
    WHERE event_kind IN ('safe_setup','added_owner','removed_owner')
      AND owner IS NOT NULL
    GROUP BY safe_address, owner
),

current_threshold AS (
    SELECT
        safe_address,
        argMax(threshold, (block_number, log_index)) AS latest_threshold
    FROM `dbt`.`int_execution_safes_owner_events`
    WHERE event_kind IN ('safe_setup','changed_threshold')
      AND threshold IS NOT NULL
    GROUP BY safe_address
)

SELECT
    ol.safe_address,
    ol.owner,
    ol.last_event_time           AS became_owner_at,
    ct.latest_threshold          AS current_threshold
FROM owner_latest ol
LEFT JOIN current_threshold ct USING (safe_address)
WHERE ol.last_event_kind IN ('safe_setup','added_owner')