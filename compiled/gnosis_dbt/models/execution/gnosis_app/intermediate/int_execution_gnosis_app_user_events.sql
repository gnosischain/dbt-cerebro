
-- heuristic_kind MUST be in order_by: a single tx can trigger multiple
-- rules for the same user (e.g. circles_personal_mint + circles_fee on
-- the same mint claim), and ReplacingMergeTree dedups by order_by. The
-- unique_key here only governs the delete+insert deletion semantics, not
-- on-disk replacement — leaving heuristic_kind out of order_by would
-- silently collapse all but one heuristic per (address, block_timestamp,
-- transaction_hash) tuple.





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
    AND tx.block_timestamp < today()
    
      
  
    
    
    
    
    
    

    AND 
    
      
      toStartOfMonth(toDate(tx.block_timestamp)) >= (
        SELECT toStartOfMonth(addDays(max(toDate(x1.block_timestamp)), -0))
        FROM `dbt`.`int_execution_gnosis_app_user_events` AS x1
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

-- Rule 2: CRC ERC-1155 transfer to the Gnosis App fee receiver via Cometh tx.
-- ("Metri" was rebranded to "Gnosis App" — the receiver address is unchanged.)
-- contracts_circles_v2_Hub_events stores transaction_hash WITHOUT 0x
-- prefix (inherited from execution.logs), so the join is direct.
rule_app_fee AS (
    SELECT
        lower(he.decoded_params['from'])  AS address,
        'circles_fee'                     AS heuristic_kind,
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
),

-- Rule 7: Personal CRC mint via Cometh tx. Sourced from
-- int_execution_circles_v2_mint_events with mint_kind = 'personal'
-- (Human token owner — covers both self-token claims and cross-token
-- matrix-routed unscheduled mints; excludes group mints and V1→V2
-- migrations via the dedicated Migration contract operator). The
-- address credited is the **token owner** (the minter), not the
-- recipient — for cross-token mints the recipient is just a routing
-- counterparty.
--
-- mint_events stores transaction_hash WITH a 0x prefix (inherited from
-- hub_transfers), while cometh_txs is from execution.transactions and
-- has no prefix — so we strip 0x in the join. Output is already
-- 0x-prefixed and matches the other rules' format.
rule_personal_mint AS (
    SELECT
        lower(m.token_address)           AS address,
        'circles_personal_mint'          AS heuristic_kind,
        m.block_timestamp,
        m.transaction_hash               AS transaction_hash
    FROM `dbt`.`int_execution_circles_v2_mint_events` m
    INNER JOIN cometh_txs ct
        ON replaceAll(m.transaction_hash, '0x', '') = ct.transaction_hash
    WHERE m.mint_kind = 'personal'
)

SELECT * FROM (
    SELECT * FROM rule_safe_invitation
    UNION ALL SELECT * FROM rule_app_fee
    UNION ALL SELECT * FROM rule_register_human
    UNION ALL SELECT * FROM rule_invite_human
    UNION ALL SELECT * FROM rule_trust
    UNION ALL SELECT * FROM rule_profile
    UNION ALL SELECT * FROM rule_personal_mint
)
WHERE address IS NOT NULL
  AND address != ''
  AND address != '0x0000000000000000000000000000000000000000'