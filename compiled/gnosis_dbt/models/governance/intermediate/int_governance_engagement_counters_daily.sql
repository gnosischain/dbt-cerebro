

-- Daily snapshots of the two forum counters that CANNOT be recovered retroactively.
--
-- Discourse exposes `views` (topic) and `reads` (post) only as current totals, with no
-- per-event endpoint and no timestamps. Worse, daily ingestion re-reads a topic only
-- when its bumped_at beats the watermark, so a dormant thread's counters are frozen at
-- its last activity -- they are not even "now". Without this model there is exactly one
-- observation per entity, forever, and questions like "how much attention did this get
-- DURING the vote" are permanently unanswerable.
--
-- Likes are deliberately NOT snapshotted here. They have a per-event endpoint
-- (/user_actions.json), so the full like history including timestamps and actors is
-- backfillable into forum_likes -- see stg_governance__forum_likes. Snapshotting a like
-- counter would be strictly worse information at higher cost.
--
-- THIS CANNOT BE BACKFILLED. History starts the day it first runs; every day it does not
-- run is permanently absent. That is the entire argument for starting it early.
--
-- PARTITION BY snapshot_date, deliberately not toStartOfMonth. With insert_overwrite,
-- dbt replaces whole partitions present in the new batch -- and each run emits only
-- today's rows. A month-grain partition would therefore wipe every earlier day in the
-- current month on every run. Daily partitions make each run overwrite exactly its own
-- day, which is what makes re-running safely idempotent.
--
-- snapshot_date is the day we LOOKED; observed_at is the source row's ingested_at, i.e.
-- when the value was actually true. Keeping both is what lets a reader tell a fresh
-- reading from a stale one rather than trusting a regular grid that may be repeating a
-- months-old number.

WITH topic_views AS (
    SELECT
        toDate(now())                    AS snapshot_date,
        CAST('topic' AS String)          AS entity_kind,
        toUInt64(id)                     AS entity_id,
        CAST('views' AS String)          AS metric,
        toUInt64(views)                  AS value,
        ingested_at                      AS observed_at
    FROM `dbt`.`stg_governance__forum_topics`
),

post_reads AS (
    SELECT
        toDate(now())                    AS snapshot_date,
        CAST('post' AS String)           AS entity_kind,
        toUInt64(id)                     AS entity_id,
        CAST('reads' AS String)          AS metric,
        toUInt64(reads)                  AS value,
        ingested_at                      AS observed_at
    FROM `dbt`.`stg_governance__forum_posts`
)

SELECT snapshot_date, entity_kind, entity_id, metric, value, observed_at FROM topic_views
UNION ALL
SELECT snapshot_date, entity_kind, entity_id, metric, value, observed_at FROM post_reads