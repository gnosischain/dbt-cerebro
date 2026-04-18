





WITH cometh_txs AS (
    -- Narrow execution.transactions ONCE to the Cometh-relayed tx set per
    -- monthly batch. Every rule joins this CTE instead of re-scanning the
    -- transactions table six times.
    SELECT
        transaction_hash,
        from_address AS relayer_address,
        block_timestamp,
        block_number
    FROM `execution`.`transactions` tx
    WHERE 
    tx.to_address = '0000000071727de22e5e9d8baf0edac6f37da032'
    AND lower(tx.from_address) IN (
        SELECT lower(replaceAll(address, '0x', ''))
        FROM `dbt`.`gnosis_app_relayers`
        WHERE is_active = 1
    )
    AND tx.block_timestamp >= toDateTime('2025-11-12')
    
      
  
    
    

   AND 
    toStartOfMonth(toDate(tx.block_timestamp)) >= (
      SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
      FROM `dbt`.`int_execution_gnosis_app_user_events` AS x1
      WHERE 1=1 
    )
    AND toDate(tx.block_timestamp) >= (
      SELECT 
        
          addDays(max(toDate(x2.block_timestamp)), -0)
        

      FROM `dbt`.`int_execution_gnosis_app_user_events` AS x2
      WHERE 1=1 
    )
  

    

),

-- Rule 1: Safe created + Circles InvitationModule enabled in the same tx.
-- int_execution_safes / int_execution_safes_module_events both store
-- transaction_hash with a 0x prefix, so we have to add it on the
-- cometh_txs side for the join.
rule_safe_invitation AS (
    SELECT
        s.safe_address               AS address,    -- already 0x-prefixed
        'safe_invitation_module'     AS heuristic_kind,
        least(s.block_timestamp, m.block_timestamp) AS block_timestamp,
        m.transaction_hash          AS transaction_hash
    FROM cometh_txs ct
    INNER JOIN `dbt`.`int_execution_safes` s
        ON s.tx_hash = concat('0x', ct.transaction_hash)
    INNER JOIN `dbt`.`int_execution_safes_module_events` m
        ON m.transaction_hash = concat('0x', ct.transaction_hash)
       AND m.event_kind = 'enabled_module'
       AND lower(m.target_address) = '0x00738aca013b7b2e6cfe1690f0021c3182fa40b5'
    WHERE s.block_timestamp >= toDateTime('2025-11-12')
),

-- Rule 2: CRC ERC-1155 transfer to the Metri fee receiver via Cometh tx.
-- contracts_circles_v2_Hub_events stores transaction_hash WITHOUT 0x
-- prefix (inherited from execution.logs), so the join is direct.
rule_metri_fee AS (
    SELECT
        lower(he.decoded_params['from'])  AS address,
        'circles_metri_fee'               AS heuristic_kind,
        he.block_timestamp,
        concat('0x', he.transaction_hash) AS transaction_hash
    FROM `dbt`.`contracts_circles_v2_Hub_events` he
    INNER JOIN cometh_txs ct
        ON he.transaction_hash = ct.transaction_hash
    WHERE he.event_name IN ('TransferSingle','TransferBatch')
      AND lower(he.decoded_params['to']) = '0x97fd8f7829a019946329f6d2e763a72741047518'
),

-- Rule 3: Hub.RegisterHuman via Cometh tx.
rule_register_human AS (
    SELECT
        lower(he.decoded_params['avatar']) AS address,
        'circles_register_human'           AS heuristic_kind,
        he.block_timestamp,
        concat('0x', he.transaction_hash) AS transaction_hash
    FROM `dbt`.`contracts_circles_v2_Hub_events` he
    INNER JOIN cometh_txs ct
        ON he.transaction_hash = ct.transaction_hash
    WHERE he.event_name = 'RegisterHuman'
),

-- Rule 4: Inviter of a Hub.RegisterHuman via Cometh tx.
rule_invite_human AS (
    SELECT
        lower(he.decoded_params['inviter']) AS address,
        'circles_invite_human'              AS heuristic_kind,
        he.block_timestamp,
        concat('0x', he.transaction_hash) AS transaction_hash
    FROM `dbt`.`contracts_circles_v2_Hub_events` he
    INNER JOIN cometh_txs ct
        ON he.transaction_hash = ct.transaction_hash
    WHERE he.event_name = 'RegisterHuman'
      AND he.decoded_params['inviter'] IS NOT NULL
      AND he.decoded_params['inviter'] != ''
      AND he.decoded_params['inviter'] != '0x0000000000000000000000000000000000000000'
),

-- Rule 5: Hub.Trust via Cometh tx — the truster is the user.
rule_trust AS (
    SELECT
        lower(he.decoded_params['truster']) AS address,
        'circles_trust'                     AS heuristic_kind,
        he.block_timestamp,
        concat('0x', he.transaction_hash) AS transaction_hash
    FROM `dbt`.`contracts_circles_v2_Hub_events` he
    INNER JOIN cometh_txs ct
        ON he.transaction_hash = ct.transaction_hash
    WHERE he.event_name = 'Trust'
),

-- Rule 6: NameRegistry.UpdateMetadataDigest via Cometh tx.
rule_profile AS (
    SELECT
        lower(nre.decoded_params['avatar']) AS address,
        'circles_profile_update'            AS heuristic_kind,
        nre.block_timestamp,
        concat('0x', nre.transaction_hash) AS transaction_hash
    FROM `dbt`.`contracts_circles_v2_NameRegistry_events` nre
    INNER JOIN cometh_txs ct
        ON nre.transaction_hash = ct.transaction_hash
    WHERE nre.event_name = 'UpdateMetadataDigest'
)

SELECT * FROM (
    SELECT * FROM rule_safe_invitation
    UNION ALL SELECT * FROM rule_metri_fee
    UNION ALL SELECT * FROM rule_register_human
    UNION ALL SELECT * FROM rule_invite_human
    UNION ALL SELECT * FROM rule_trust
    UNION ALL SELECT * FROM rule_profile
)
WHERE address IS NOT NULL
  AND address != ''
  AND address != '0x0000000000000000000000000000000000000000'