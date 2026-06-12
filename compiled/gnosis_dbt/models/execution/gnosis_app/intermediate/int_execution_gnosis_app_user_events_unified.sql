



-- Thin UNION ALL of the chain-side and Mixpanel-side unified event logs.
-- The single touchpoint table the MTA persona's runtime mapping points
-- at as `touchpoint_model`. Reads from already-incremental upstream
-- intermediates, so this model is a low-cost reshuffle.
--
-- Chain rows have NULL for Mixpanel-only columns (device_type,
-- country_code, page_path, bottom_sheet); Mixpanel rows have NULL for
-- amount_usd. event_kind drives all attribution downstream.

SELECT
    event_ts,
    event_date,
    user_pseudonym,
    event_source,
    event_kind,
    event_subkind,
    amount_usd,
    event_dedup_key,
    provenance_model,
    CAST(NULL AS Nullable(String)) AS device_type,
    CAST(NULL AS Nullable(String)) AS country_code,
    CAST(NULL AS Nullable(String)) AS page_path,
    CAST(NULL AS Nullable(String)) AS bottom_sheet
FROM `dbt`.`int_execution_gnosis_app_events_chain_unified`
WHERE 1=1

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_user_events_unified` AS x1
        WHERE 1=1 
      )
    
  



UNION ALL

SELECT
    event_ts,
    event_date,
    user_pseudonym,
    event_source,
    event_kind,
    event_subkind,
    amount_usd,
    event_dedup_key,
    provenance_model,
    device_type,
    country_code,
    page_path,
    bottom_sheet
FROM `dbt`.`int_execution_gnosis_app_events_mixpanel_unified`
WHERE 1=1

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_user_events_unified` AS x1
        WHERE 1=1 
      )
    
  

