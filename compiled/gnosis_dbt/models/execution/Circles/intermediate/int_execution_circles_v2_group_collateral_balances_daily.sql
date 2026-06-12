



WITH deltas AS (
    SELECT
        toDate(block_timestamp) AS date,
        group_address,
        token_id,
        sum(delta_raw) AS net_delta_raw
    FROM `dbt`.`int_execution_circles_v2_group_collateral_diffs`
    WHERE toDate(block_timestamp) < today()
    
      
  

    
    GROUP BY 1, 2, 3
),

with_running_totals AS (
    SELECT
        d.date,
        d.group_address,
        d.token_id,
        d.net_delta_raw,
        sum(d.net_delta_raw) OVER (
            PARTITION BY d.group_address, d.token_id
            ORDER BY d.date
        )
         AS balance_raw
    FROM deltas d
    
)

SELECT
    date,
    group_address,
    token_id,
    balance_raw
FROM with_running_totals
WHERE balance_raw > 0