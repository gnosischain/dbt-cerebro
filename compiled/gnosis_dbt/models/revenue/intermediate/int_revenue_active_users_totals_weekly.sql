






WITH per_user AS (
    SELECT
        week,
        user,
        sum(annual_rolling_fees) AS annual_rolling_fees
    FROM `dbt`.`int_revenue_fees_weekly_per_user`
    
    
  
    
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(week)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.week)), -34))
        FROM `dbt`.`int_revenue_active_users_totals_weekly` AS x1
        WHERE 1=1 
      )
      
    
  

    
    GROUP BY week, user
)

SELECT
    week,
    countIf(annual_rolling_fees >= 6.0) AS users_cnt,
    round(sumIf(annual_rolling_fees, annual_rolling_fees >= 6.0), 2) AS annual_rolling_fees_total
FROM per_user
GROUP BY week