




SELECT
    event_date                      AS date,
    hour_of_day,
    day_of_week,
    count()                         AS event_count,
    uniqExact(user_id_hash)         AS unique_users
FROM `dbt`.`stg_mixpanel_ga__events`
WHERE event_date < today()
  AND is_production = 1
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_mixpanel_ga_usage_patterns_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_mixpanel_ga_usage_patterns_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, hour_of_day, day_of_week