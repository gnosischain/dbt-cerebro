



-- Long-form Mixpanel event log filtered to identified, production traffic
-- and joined to the GA user identity bridge so only events from users in
-- the GA cohort flow through. Anonymous Mixpanel visitors and non-GA
-- identified users are excluded.
--
-- Joins on user_pseudonym = stg_mixpanel_ga__events.user_id_hash, which
-- works because both sides apply the same `pseudonymize_address` macro
-- with the same salt (deterministic).

WITH bridge AS (
    SELECT user_pseudonym
    FROM `dbt`.`int_execution_gnosis_app_user_identity_bridge`
)

SELECT
    e.event_time                                            AS event_ts,
    e.event_date                                            AS event_date,
    e.user_id_hash                                          AS user_pseudonym,
    'mixpanel'                                              AS event_source,
    concat('mp.', e.event_category)                         AS event_kind,
    e.event_name                                            AS event_subkind,
    CAST(NULL AS Nullable(Float64))                         AS amount_usd,
    cityHash64(e.insert_id)                                 AS event_dedup_key,
    'stg_mixpanel_ga__events'                               AS provenance_model,
    e.device_type                                           AS device_type,
    e.country_code                                          AS country_code,
    e.page_path                                             AS page_path,
    e.bottom_sheet                                          AS bottom_sheet
FROM `dbt`.`stg_mixpanel_ga__events` e
INNER JOIN bridge b ON b.user_pseudonym = e.user_id_hash
WHERE e.is_production = 1
  AND e.is_identified = 1
  AND e.event_date < today()

  
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(e.event_date)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_mixpanel_unified` AS x1
        WHERE 1=1 
      )
      AND toDate(e.event_date) >= (
        SELECT
          
            addDays(max(toDate(x2.event_date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_events_mixpanel_unified` AS x2
        WHERE 1=1 
      )
    
  

