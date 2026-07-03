

-- New identity / profile REGISTRATIONS per month, from the avatar registration
-- timestamp (avatar.created_at). 100% of avatars carry a profile_id, so avatar
-- registration = account/profile creation (the `profile` table itself has no
-- creation field, only last_updated_block_number). Spans Circles v1+v2 back to
-- 2020 — a broader, real onboarding series than the heuristic `new_users`
-- (new-to-current-app-bundler). Split by protocol version.
SELECT
    toStartOfMonth(created_at)      AS month,
    count()                         AS new_registrations,
    countIf(circles_version = 2)    AS v2_registrations,
    countIf(circles_version = 1)    AS v1_registrations
FROM `dbt`.`stg_envio_ga__avatars`
WHERE created_at > toDateTime('2019-01-01 00:00:00')
GROUP BY month