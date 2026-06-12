

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_circles_v2_avatars`) AS as_of_date
FROM (
-- Bucketed histogram of Circles v2 group sizes (members per group).
-- One row per bucket; buckets ordered low → high via bucket_order.

-- The dashboard BarChart sorts categories alphabetically with no opt-out,
-- so labels are prefixed with their bucket_order to coerce natural order
-- when sorted as strings ("1. 0" < "2. 1–5" < ... < "6. 500+").

SELECT
    multiIf(
        n_members = 0,    '1. 0',
        n_members <= 5,   '2. 1–5',
        n_members <= 20,  '3. 6–20',
        n_members <= 100, '4. 21–100',
        n_members <= 500, '5. 101–500',
                          '6. 500+'
    ) AS bucket,
    multiIf(
        n_members = 0,    toUInt8(1),
        n_members <= 5,   toUInt8(2),
        n_members <= 20,  toUInt8(3),
        n_members <= 100, toUInt8(4),
        n_members <= 500, toUInt8(5),
                          toUInt8(6)
    ) AS bucket_order,
    count() AS n_groups
FROM `dbt`.`fct_execution_circles_v2_group_size_current`
GROUP BY bucket, bucket_order
ORDER BY bucket_order
) AS sub