




SELECT
    event_date                      AS date,
    user_id_hash,
    count()                         AS event_count,
    uniqExact(event_name)           AS distinct_event_types,
    uniqExact(page_path)            AS distinct_pages,
    min(event_time)                 AS first_event_time,
    max(event_time)                 AS last_event_time,
    max(is_identified)              AS is_identified,
    uniqExact(device_id_hash)       AS unique_devices
FROM `dbt`.`stg_mixpanel_ga__events`
WHERE event_date < today()
  AND is_production = 1
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_mixpanel_ga_users_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_mixpanel_ga_users_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, user_id_hash