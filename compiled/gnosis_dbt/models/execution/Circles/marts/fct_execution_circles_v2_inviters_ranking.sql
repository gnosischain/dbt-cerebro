

SELECT
    invited_by,
    count() AS invite_count,
    min(block_timestamp) AS first_invite_ts,
    max(block_timestamp) AS last_invite_ts,
    row_number() OVER (ORDER BY count() DESC) AS rank
FROM `dbt`.`int_execution_circles_v2_avatars`
WHERE avatar_type = 'Human'
  AND invited_by IS NOT NULL
  AND invited_by != '0x0000000000000000000000000000000000000000'
GROUP BY invited_by
ORDER BY invite_count DESC