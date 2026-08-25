

SELECT
    id,
    parent_id,
    name,
    slug,
    topic_count,
    post_count,
    description,
    ingested_at
FROM `governance_db`.`forum_categories` FINAL