




SELECT
    event_date                      AS date,
    current_domain,
    page_path,
    count()                         AS event_count,
    uniqExact(user_id_hash)         AS unique_users
FROM `dbt`.`stg_mixpanel_ga__events`
WHERE page_path != ''
  AND event_date < today()
  AND is_production = 1
  
    
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_mixpanel_ga_pages_daily` AS x1
        WHERE 1=1 
      )
      AND toDate(event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.date)), -0)
          

        FROM `dbt`.`int_mixpanel_ga_pages_daily` AS x2
        WHERE 1=1 
      )
    
  

  
GROUP BY date, current_domain, page_path