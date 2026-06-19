

-- Daily count of every Circles v2 Hub event, broken down by event_name.
-- One row per (date, event_name) with:
--   n_events             - row count of that event on that day
--   n_tx                 - distinct transactions emitting the event
--   n_distinct_addresses - distinct addresses across all event-specific
--                          participant fields (avatar/inviter/group/org/
--                          truster/trustee/operator/from/to/human/account/
--                          backer/holder). The 0x00..00 sentinel and the
--                          empty string are excluded.
--
-- Drives the event-mix heatmap on the Circles dashboard.




SELECT
    toDate(e.block_timestamp)              AS date,
    e.event_name                           AS event_name,
    countDistinct(e.transaction_hash, e.log_index) AS n_events,
    uniqExact(e.transaction_hash)          AS n_tx,
    uniqExactIf(participant, participant != '') AS n_distinct_addresses
FROM `dbt`.`contracts_circles_v2_Hub_events` e
LEFT ARRAY JOIN
    arrayFilter(
        x -> x != '' AND x != '0x0000000000000000000000000000000000000000',
        arrayMap(addr -> lower(addr), [
            coalesce(e.decoded_params['avatar'],       ''),
            coalesce(e.decoded_params['inviter'],      ''),
            coalesce(e.decoded_params['group'],        ''),
            coalesce(e.decoded_params['organization'], ''),
            coalesce(e.decoded_params['truster'],      ''),
            coalesce(e.decoded_params['trustee'],      ''),
            coalesce(e.decoded_params['operator'],     ''),
            coalesce(e.decoded_params['from'],         ''),
            coalesce(e.decoded_params['to'],           ''),
            coalesce(e.decoded_params['human'],        ''),
            coalesce(e.decoded_params['account'],      ''),
            coalesce(e.decoded_params['backer'],       ''),
            coalesce(e.decoded_params['holder'],       '')
        ])
    ) AS participant
WHERE e.block_timestamp < today()
  
    
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(e.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.date)), -0))
        FROM `dbt`.`int_execution_circles_v2_hub_events_daily` AS x1
        WHERE 1=1 
      )
      
    
  

  
GROUP BY date, event_name