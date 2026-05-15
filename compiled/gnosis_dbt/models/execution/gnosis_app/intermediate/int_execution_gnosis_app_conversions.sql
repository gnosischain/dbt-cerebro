



-- Conversion registry — one row per conversion event, conversion_kind as
-- a column. The MTA persona's runtime mapping points at this as
-- `conversion_model`. Persona filters with WHERE conversion_kind = '<kind>'
-- to swap between conversion targets without changing SQL shape.

WITH bridge AS (
    SELECT address, user_pseudonym
    FROM `dbt`.`int_execution_gnosis_app_user_identity_bridge`
),

topup_rows AS (
    SELECT
        toDateTime(t.block_timestamp)                        AS conversion_ts,
        toDate(t.block_timestamp)                            AS conversion_date,
        b.user_pseudonym                                     AS user_pseudonym,
        'topup'                                              AS conversion_kind,
        toFloat64OrNull(toString(t.amount_usd))              AS conversion_amount_usd,
        t.token_bought_symbol                                AS conversion_token,
        cityHash64('topup', t.transaction_hash, toString(t.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_gpay_topups'               AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_gpay_topups` t
    INNER JOIN bridge b ON b.address = lower(t.ga_user)
    WHERE 1=1
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x1
        WHERE 1=1 
      )
      AND toDate(t.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x2
        WHERE 1=1 
      )
    
  

    
),

swap_filled_rows AS (
    SELECT
        toDateTime(assumeNotNull(s.first_fill_at))               AS conversion_ts,
        toDate(assumeNotNull(s.first_fill_at))                   AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'swap_filled'                                            AS conversion_kind,
        toFloat64OrNull(toString(s.amount_usd))                  AS conversion_amount_usd,
        CAST(NULL AS Nullable(String))                           AS conversion_token,
        cityHash64('swap_filled', s.order_uid)                   AS conversion_dedup_key,
        'int_execution_gnosis_app_swaps'                         AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_swaps` s
    INNER JOIN bridge b ON b.address = lower(s.taker)
    WHERE s.was_filled = 1
      AND s.first_fill_at IS NOT NULL
      AND s.first_fill_at < today()
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(s.first_fill_at)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x1
        WHERE 1=1 
      )
      AND toDate(s.first_fill_at) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x2
        WHERE 1=1 
      )
    
  

    
),

claim_rows AS (
    SELECT
        toDateTime(tc.block_timestamp)                           AS conversion_ts,
        toDate(tc.block_timestamp)                               AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'token_offer_claim'                                      AS conversion_kind,
        toFloat64OrNull(toString(tc.amount_received_usd))        AS conversion_amount_usd,
        tc.offer_token_symbol                                    AS conversion_token,
        cityHash64('token_offer_claim', tc.transaction_hash, toString(tc.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_token_offer_claims'            AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_token_offer_claims` tc
    INNER JOIN bridge b ON b.address = lower(tc.ga_user)
    WHERE tc.block_timestamp < today()
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(tc.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x1
        WHERE 1=1 
      )
      AND toDate(tc.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x2
        WHERE 1=1 
      )
    
  

    
),

marketplace_rows AS (
    SELECT
        toDateTime(mp.block_timestamp)                           AS conversion_ts,
        toDate(mp.block_timestamp)                               AS conversion_date,
        b.user_pseudonym                                         AS user_pseudonym,
        'marketplace_buy'                                        AS conversion_kind,
        CAST(NULL AS Nullable(Float64))                          AS conversion_amount_usd,
        mp.offer_name                                            AS conversion_token,
        cityHash64('marketplace_buy', mp.transaction_hash, toString(mp.log_index)) AS conversion_dedup_key,
        'int_execution_gnosis_app_marketplace_payments'          AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` mp
    INNER JOIN bridge b ON b.address = lower(mp.payer)
    WHERE mp.block_timestamp < today()
    
      
  
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(mp.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.conversion_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x1
        WHERE 1=1 
      )
      AND toDate(mp.block_timestamp) >= (
        SELECT
          
            addDays(max(toDate(x2.conversion_date)), -0)
          

        FROM `dbt`.`int_execution_gnosis_app_conversions` AS x2
        WHERE 1=1 
      )
    
  

    
)

SELECT * FROM topup_rows
UNION ALL SELECT * FROM swap_filled_rows
UNION ALL SELECT * FROM claim_rows
UNION ALL SELECT * FROM marketplace_rows