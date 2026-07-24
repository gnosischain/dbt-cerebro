






WITH per_user AS (
    SELECT
        month,
        user,
        sum(month_fees) AS month_fees
    FROM `dbt`.`int_revenue_fees_monthly_per_user`
    
    
  
    
    
    
    
    
    

    WHERE 
    
      
      toStartOfMonth(toDate(month)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.month)), -1))
        FROM `dbt`.`int_revenue_active_users_totals_monthly` AS x1
        WHERE 1=1 
      )
      
    
  

    
    GROUP BY month, user
)

SELECT
    month,
    countIf(month_fees >= 0.5) AS users_cnt,
    round(sumIf(month_fees, month_fees >= 0.5), 2) AS fees_total
FROM per_user
GROUP BY month