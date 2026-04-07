

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    avatar_type,
    invited_by,
    avatar,
    token_id,
    name
FROM `dbt`.`int_execution_circles_v2_avatars`
ORDER BY block_timestamp DESC