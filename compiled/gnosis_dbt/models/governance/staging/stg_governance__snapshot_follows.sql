

SELECT
    id,
    lower(follower) AS follower,
    space_id,
    created_at,
    ingested_at
FROM `governance_db`.`snapshot_follows` FINAL