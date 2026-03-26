{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'tokens']
    )
}}

SELECT
    argMax(block_number, tuple(block_number, transaction_index, log_index)) AS block_number,
    argMax(block_timestamp, tuple(block_number, transaction_index, log_index)) AS block_timestamp,
    argMax(transaction_hash, tuple(block_number, transaction_index, log_index)) AS transaction_hash,
    argMax(transaction_index, tuple(block_number, transaction_index, log_index)) AS transaction_index,
    argMax(log_index, tuple(block_number, transaction_index, log_index)) AS log_index,
    argMax(version, tuple(block_number, transaction_index, log_index)) AS version,
    argMax(token_type, tuple(block_number, transaction_index, log_index)) AS token_type,
    token,
    argMax(token_owner, tuple(block_number, transaction_index, log_index)) AS token_owner,
    argMax(avatar, tuple(block_number, transaction_index, log_index)) AS avatar
FROM {{ ref('int_execution_circles_tokens') }}
GROUP BY token
