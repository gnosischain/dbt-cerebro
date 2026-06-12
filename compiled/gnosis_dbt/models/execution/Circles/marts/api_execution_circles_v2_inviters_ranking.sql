

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_circles_v2_avatars`) AS as_of_date
FROM (
-- Leaderboard of top inviters by humans invited. Passthrough over
-- fct_execution_circles_v2_inviters_ranking, which pre-joins the inviter's
-- display name, preview image URL, and current blacklist flag.

SELECT
    rank,
    inviter,
    display_name,
    preview_image_url,
    is_blacklisted,
    invite_count,
    first_invite_ts,
    last_invite_ts
FROM `dbt`.`fct_execution_circles_v2_inviters_ranking`
ORDER BY rank
) AS sub