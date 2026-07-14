

-- Latest on-chain score per (group, member) from score-based group mints.
-- One row per member per score-based group; score is the value at the member's
-- most recent PersonalMinted event.
SELECT
    today()                          AS as_of_date,
    group_address                    AS group_address,
    avatar                           AS member,
    argMax(score, block_timestamp)   AS score,
    max(block_timestamp)             AS last_mint_at,
    argMax(amount, block_timestamp)  AS last_mint_amount,
    count()                          AS n_mints
FROM `dbt`.`int_execution_circles_v2_score_mints`
GROUP BY group_address, avatar