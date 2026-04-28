




SELECT
    event_date                      AS date,
    browser,
    os,
    device_type,
    count()                         AS event_count,
    uniqExact(user_id_hash)         AS unique_users,
    uniqExact(device_id_hash)       AS unique_devices
FROM `dbt`.`stg_mixpanel_ga__events`
WHERE event_date < today()
  AND is_production = 1
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_mixpanel_ga_tech_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_mixpanel_ga_tech_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, browser, os, device_type