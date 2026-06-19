



-- Long-form on-chain GA event log at full timestamp grain, keyed by
-- user_pseudonym (not raw address). Single source of chain-side touchpoints
-- for MTA. Each row is one observed action by one identified GA user.
--
-- Event taxonomy comes from seed mta_event_kinds.csv. event_kind starts
-- with 'chain.' for everything emitted here; the Mixpanel intermediate
-- emits 'mp.' prefixes. The two intermediates UNION ALL into
-- int_execution_gnosis_app_user_events_unified.

WITH bridge AS (
    SELECT address, user_pseudonym
    FROM `dbt`.`int_execution_gnosis_app_user_identity_bridge`
),

-- 1. onboard — first-ever heuristic hit per user
onboard_rows AS (
    SELECT
        toDateTime(uc.first_seen_at)                    AS event_ts,
        toDate(uc.first_seen_at)                        AS event_date,
        b.user_pseudonym                                AS user_pseudonym,
        'chain'                                         AS event_source,
        'chain.onboard'                                 AS event_kind,
        'onboard'                                       AS event_subkind,
        CAST(NULL AS Nullable(Float64))                 AS amount_usd,
        cityHash64('onboard', toString(b.user_pseudonym)) AS event_dedup_key,
        'int_execution_gnosis_app_users_current'        AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_users_current` uc
    INNER JOIN bridge b ON b.address = lower(uc.address)
    WHERE uc.first_seen_at IS NOT NULL
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(uc.first_seen_at)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 2. heuristic events — circles_register_human, circles_invite_human,
--    circles_trust, circles_profile_update, circles_fee,
--    circles_personal_mint, safe_invitation_module — all flow through
--    int_execution_gnosis_app_user_events
heuristic_rows AS (
    SELECT
        toDateTime(ue.block_timestamp)                          AS event_ts,
        toDate(ue.block_timestamp)                              AS event_date,
        b.user_pseudonym                                        AS user_pseudonym,
        'chain'                                                 AS event_source,
        concat('chain.', ue.heuristic_kind)                     AS event_kind,
        ue.heuristic_kind                                       AS event_subkind,
        CAST(NULL AS Nullable(Float64))                         AS amount_usd,
        cityHash64(ue.heuristic_kind, ue.transaction_hash, toString(b.user_pseudonym)) AS event_dedup_key,
        'int_execution_gnosis_app_user_events'                  AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_user_events` ue
    INNER JOIN bridge b ON b.address = lower(ue.address)
    WHERE ue.block_timestamp IS NOT NULL
      AND ue.block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(ue.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 3. swap_signed (PreSignature events; one per swap)
swap_signed_rows AS (
    SELECT
        toDateTime(s.block_timestamp)                           AS event_ts,
        toDate(s.block_timestamp)                               AS event_date,
        b.user_pseudonym                                        AS user_pseudonym,
        'chain'                                                 AS event_source,
        'chain.swap_signed'                                     AS event_kind,
        'swap_signed'                                           AS event_subkind,
        CAST(NULL AS Nullable(Float64))                         AS amount_usd,
        cityHash64('swap_signed', s.order_uid)                  AS event_dedup_key,
        'int_execution_gnosis_app_swaps'                        AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_swaps` s
    INNER JOIN bridge b ON b.address = lower(s.taker)
    WHERE s.block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(s.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 4. swap_filled (was_filled subset)
swap_filled_rows AS (
    SELECT
        toDateTime(assumeNotNull(s.first_fill_at))                       AS event_ts,
        toDate(assumeNotNull(s.first_fill_at))                           AS event_date,
        b.user_pseudonym                                                 AS user_pseudonym,
        'chain'                                                          AS event_source,
        'chain.swap_filled'                                              AS event_kind,
        'swap_filled'                                                    AS event_subkind,
        toFloat64OrNull(toString(s.amount_usd))                          AS amount_usd,
        cityHash64('swap_filled', s.order_uid)                           AS event_dedup_key,
        'int_execution_gnosis_app_swaps'                                 AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_swaps` s
    INNER JOIN bridge b ON b.address = lower(s.taker)
    WHERE s.was_filled = 1
      AND s.first_fill_at IS NOT NULL
      AND s.first_fill_at < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(s.first_fill_at)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 5. topup
topup_rows AS (
    SELECT
        toDateTime(t.block_timestamp)                           AS event_ts,
        toDate(t.block_timestamp)                               AS event_date,
        b.user_pseudonym                                        AS user_pseudonym,
        'chain'                                                 AS event_source,
        'chain.topup'                                           AS event_kind,
        'topup'                                                 AS event_subkind,
        toFloat64OrNull(toString(t.amount_usd))                 AS amount_usd,
        cityHash64('topup', t.transaction_hash, toString(t.log_index)) AS event_dedup_key,
        'int_execution_gnosis_app_gpay_topups'                  AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_gpay_topups` t
    INNER JOIN bridge b ON b.address = lower(t.ga_user)
    WHERE 1=1
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(t.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 6. marketplace_buy
marketplace_rows AS (
    SELECT
        toDateTime(mp.block_timestamp)                          AS event_ts,
        toDate(mp.block_timestamp)                              AS event_date,
        b.user_pseudonym                                        AS user_pseudonym,
        'chain'                                                 AS event_source,
        'chain.marketplace_buy'                                 AS event_kind,
        'marketplace_buy'                                       AS event_subkind,
        CAST(NULL AS Nullable(Float64))                         AS amount_usd,
        cityHash64('marketplace_buy', mp.transaction_hash, toString(mp.log_index)) AS event_dedup_key,
        'int_execution_gnosis_app_marketplace_payments'         AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_marketplace_payments` mp
    INNER JOIN bridge b ON b.address = lower(mp.payer)
    WHERE mp.block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(mp.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
),

-- 7. token_offer_claim
claim_rows AS (
    SELECT
        toDateTime(tc.block_timestamp)                                  AS event_ts,
        toDate(tc.block_timestamp)                                      AS event_date,
        b.user_pseudonym                                                AS user_pseudonym,
        'chain'                                                         AS event_source,
        'chain.token_offer_claim'                                       AS event_kind,
        'token_offer_claim'                                             AS event_subkind,
        toFloat64OrNull(toString(tc.amount_received_usd))               AS amount_usd,
        cityHash64('token_offer_claim', tc.transaction_hash, toString(tc.log_index)) AS event_dedup_key,
        'int_execution_gnosis_app_token_offer_claims'                   AS provenance_model
    FROM `dbt`.`int_execution_gnosis_app_token_offer_claims` tc
    INNER JOIN bridge b ON b.address = lower(tc.ga_user)
    WHERE tc.block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(tc.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.event_date)), -0))
        FROM `dbt`.`int_execution_gnosis_app_events_chain_unified` AS x1
        WHERE 1=1 
      )
      
    
  

    
)

SELECT * FROM onboard_rows
UNION ALL SELECT * FROM heuristic_rows
UNION ALL SELECT * FROM swap_signed_rows
UNION ALL SELECT * FROM swap_filled_rows
UNION ALL SELECT * FROM topup_rows
UNION ALL SELECT * FROM marketplace_rows
UNION ALL SELECT * FROM claim_rows