


WITH


state_size_diff AS (
    SELECT 
        toStartOfDay(block_timestamp) AS date 
        ,SUM(IF(to_value!='0000000000000000000000000000000000000000000000000000000000000000',32,-32)) AS bytes_diff
    FROM 
        `dbt`.`stg_execution__storage_diffs`
    
  
    
    

   WHERE 
    toStartOfMonth(toDate(block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
      FROM `dbt`.`int_execution_state_size_full_diff_daily` AS x1
      WHERE 1=1 
    )
    AND toDate(block_timestamp) >= (
      SELECT addDays(max(toDate(x2.date)), -0)
      FROM `dbt`.`int_execution_state_size_full_diff_daily` AS x2
      WHERE 1=1 
    )
  

    GROUP BY 1
)

SELECT
    *
FROM state_size_diff