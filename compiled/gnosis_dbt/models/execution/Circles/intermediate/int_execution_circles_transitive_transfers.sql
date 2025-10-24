


SELECT
  toStartOfDay(block_timestamp) AS date
  ,decoded_params['from'] AS from_avatar
  ,decoded_params['to']   AS  to_avatar
  ,SUM(
    toUInt256OrZero(
      arrayJoin(
        JSONExtract(
          ifNull(decoded_params['amounts'], '[]'),   -- remove Nullable
          'Array(String)'                            -- get Array(String)
        )
      )
    )
  ) AS total_amount
  ,COUNT(*) AS cnt
FROM `dbt`.`contracts_circles_v2_Hub_events`
WHERE
  event_name = 'StreamCompleted'
  
  
    
      
    

   AND 
    toStartOfMonth(toStartOfDay(block_timestamp)) >= (
      SELECT max(toStartOfMonth(t.date))
      FROM `dbt`.`int_execution_circles_transitive_transfers` AS t
    )
    AND toStartOfDay(block_timestamp) >= (
      SELECT max(toStartOfDay(t2.date, 'UTC'))
      FROM `dbt`.`int_execution_circles_transitive_transfers` AS t2
    )
  

GROUP BY 1, 2, 3