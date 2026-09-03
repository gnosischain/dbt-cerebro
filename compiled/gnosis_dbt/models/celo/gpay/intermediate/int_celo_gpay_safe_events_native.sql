
-- Safe lifecycle events for GP card Safes on Celo, decoded through the
-- multichain decode pipeline (chain='celo'): SafeSetup (initial owners),
-- AddedOwner, RemovedOwner. The ABI is resolved from the GnosisSafe v1.3.0 L2
-- singleton row in the celo partition of the signature seeds
-- (abi_source_address override — the events live on the proxies, the ABI on
-- the singleton; SafeSetup/AddedOwner/RemovedOwner topic0s are identical
-- across Safe 1.3.0/1.4.1 so one ABI source covers both).
--
-- Incremental append + monthly partitions, mirroring Gnosis Chain's
-- int_execution_safes_owner_events. The output is tiny (bounded by card count)
-- but the COST is the scan of celo_execution.logs, which a full rebuild re-reads
-- over the whole GP era on every run — that is what OOMed once the Celo backfill
-- landed (CH code 241, OvercommitTracker victim). Append + the decode_logs
-- block_number watermark means a daily run reads only new blocks, and a rebuild
-- goes through scripts/full_refresh/refresh.py in the monthly batches declared
-- in meta.full_refresh (decode_logs honors start_month/end_month).
--
-- Consequence of the watermark, same as every other append decode stream here
-- (contracts_celo_chainlink_feeds_events documents the identical hazard): logs
-- landing in celo_execution BELOW the high-water mark are never decoded. The
-- backfill is complete, so the remaining trigger is registry growth: whenever
-- int_celo_gpay_safe_registry discovers a Safe whose SafeSetup predates the
-- watermark, the affected months must be re-decoded explicitly — drop those
-- partitions first (macros/db/drop_partition.sql), because appending over a
-- populated month duplicates rows and int_celo_gpay_wallet_events reads this
-- table without FINAL (docs/lessons/append-over-populated-duplicates.md).
--
-- Every projected column is explicitly aliased: enable_analyzer = 0 (needed for
-- decode planning speed) names a bare `d.block_timestamp` as `d.block_timestamp`
-- in the result header, which the partition key and order_by could not resolve.
--
-- owner IS PART OF order_by BECAUSE IT IS PART OF THE GRAIN. A SafeSetup with N
-- owners emits N rows that differ ONLY by owner, and ReplacingMergeTree dedupes on
-- the ORDER BY key — without owner, a merge would silently collapse a multi-owner
-- setup to a single owner, losing co-owners with no error. It matches unique_key
-- for the same reason. Harmless so far (all 1503 setups have exactly one owner on
-- 2026-08-05, so the two keys are equal at 1503 each), which is exactly why it
-- would have failed quietly the first time a multi-sig card was issued.
-- NOTE: an order_by change only takes effect when the table is RECREATED, and this
-- model must never be full-refreshed directly (see the OOM warning above) — the
-- existing table keeps the old key until a staged rebuild through
-- scripts/full_refresh/refresh.py.

WITH decoded AS (
    SELECT * FROM (
        



  
    
    
  

  
  
  
    
    
    
  

  
  
    
  









  
  
    
  
    
  
    
  
  




WITH

logs AS (
  SELECT * FROM (
    SELECT *,
      row_number() OVER (
        PARTITION BY block_number, transaction_index, log_index
        ORDER BY insert_version DESC
      ) AS _dedup_rn
    FROM `celo_execution`.`logs`
    WHERE lower(replaceAll(address, '0x', '')) IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM `dbt`.`int_celo_gpay_safe_registry` cw WHERE cw.contract_type = 'SafeProxy')

      
        AND replaceAll(lower(topic0),'0x','') IN (SELECT replaceAll(lower(signature),'0x','') FROM `dbt`.`event_signatures` WHERE chain = 'celo' AND event_name IN ('SafeSetup', 'AddedOwner', 'RemovedOwner'))
      

      
        AND block_timestamp >= toDateTime('2026-01-01')
      

      
      

      
      
        
        
          
          
          
        
        
        AND block_number > 76508360
        AND block_timestamp >= toDateTime('2026-09-03 04:18:38')
        
        
        
      
  )
  WHERE _dedup_rn = 1
),


logs_with_abi AS (
  SELECT
    l.*,
    
    '3e5c63644e683549055b9be8653de26e0b4cd36e' AS abi_join_address
    
  FROM logs l
  ANY LEFT JOIN `dbt`.`int_celo_gpay_safe_registry` cw
    ON lower(replaceAll(l.address, '0x', '')) = lower(replaceAll(cw.address, '0x', ''))
     AND cw.contract_type = 'SafeProxy'
),


abi AS ( 
SELECT
  replaceAll(lower(contract_address), '0x', '')          AS abi_contract_address,
  replace(signature,'0x','')                     AS topic0_sig,
  event_name,
  arrayMap(x->JSONExtractString(x,'name'),
           JSONExtractArrayRaw(params))          AS names,
  arrayMap(x->JSONExtractString(x,'type'),
           JSONExtractArrayRaw(params))          AS types,
  arrayMap(x->JSONExtractBool(x,'indexed'),
           JSONExtractArrayRaw(params))          AS flags
FROM `dbt`.`event_signatures`
WHERE chain = 'celo'
  AND replaceAll(lower(contract_address),'0x','') = '3e5c63644e683549055b9be8653de26e0b4cd36e'
 ),

process AS (
  SELECT
    l.block_number,
    l.block_timestamp,
    l.transaction_hash,
    l.transaction_index,
    l.log_index,
    l.address           AS contract_address,
    a.event_name,

    -- ABI arrays
    a.names             AS param_names,
    a.types             AS param_types,
    a.flags             AS param_flags,
    length(a.types)     AS n_params,

    -- topics and data
    [l.topic1, l.topic2, l.topic3]       AS raw_topics,
    replaceAll(l.data,'0x','')           AS data_hex,

    -- non-indexed metadata (zip flags/types/positions, then filter non-indexed)
    arrayFilter((f,t,i) -> not f,
      arrayZip(a.flags, a.types, range(n_params))
    )                                    AS ni_meta,

    arrayMap(x -> x.2, ni_meta)          AS ni_types,
    arrayMap(x -> x.3, ni_meta)          AS ni_positions,

    -- head words (32-byte) from start of the data head area
    arrayMap(i ->
      if(i*64 < length(data_hex),
         substring(data_hex, 1 + i*64, 64),
         NULL),
      range(greatest(length(ni_types), 1) * 16)  -- generous upper bound
    )                                    AS data_words,

    -- base type for arrays (strip [])
    arrayMap(j -> replaceRegexpOne(ni_types[j+1], '\\[\\]$', ''), range(length(ni_types))) AS ni_base_types,

    /* ===================== DECODING ====================== */
    -- For each non-indexed param j return a STRING:
    --  - Arrays -> toJSONString(Array(String))
    --  - Dynamic scalars -> String (hex or utf8)
    --  - Static scalars -> String
    arrayMap(j ->
      if(
        /* -------- ARRAY TYPES -------- */
        endsWith(ni_types[j+1],'[]'),

        /* Build JSON string of the fully decoded array */
        toJSONString(
          arrayMap(
            k ->
              multiIf(
                ni_base_types[j+1] = 'address',
                  concat(
                    '0x',
                    substring(
                      substring(
                        data_hex,
                        1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                        64 + 64 + (k + 1) * 64
                      ),
                      (64 + k*64) + 25, 40
                    )
                  ),

                ni_base_types[j+1] = 'bytes32',
                  concat(
                    '0x',
                    substring(
                      data_hex,
                      1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                      64
                    )
                  ),

                startsWith(ni_base_types[j+1], 'uint')
                OR ni_base_types[j+1] = 'bool',
                  /* bool is stored as a 0/1 uint256 word, so we can use
                     the same reinterpret path as uint*. Output is '0' or
                     '1' (decimal string). */
                  toString(
                    reinterpretAsUInt256(
                      reverse(
                        unhex(
                          substring(
                            data_hex,
                            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                            64
                          )
                        )
                      )
                    )
                  ),

                  startsWith(ni_base_types[j+1], 'int'),
                  toString(
                    reinterpretAsInt256(
                      reverse(
                        unhex(
                          substring(
                            data_hex,
                            1 + toUInt32(reinterpretAsInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                            64
                          )
                        )
                      )
                    )
                  ),

                /* Fallback: full 32-byte hex */
                concat(
                  '0x',
                  substring(
                    data_hex,
                    1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64 + k*64,
                    64
                  )
                )
              ),
            /* range(N) where N is array length at base */
            range(
              toUInt32(
                reinterpretAsUInt256(
                  reverse(
                    unhex(
                      substring(
                        data_hex,
                        1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                        64
                      )
                    )
                  )
                )
              )
            )
          )
        ),

        /* -------- DYNAMIC SCALARS (string/bytes/bytesN≠32) -------- */
        if(
          ni_types[j+1] = 'bytes'
          OR ni_types[j+1] = 'string'
          OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'),

          /* payload = hex of exactly len bytes; strings converted later */
          substring(
            data_hex,
            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2 + 64,
            toUInt32(
              reinterpretAsUInt256(
                reverse(
                  unhex(
                    substring(
                      data_hex,
                      1 + toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) * 2,
                      64
                    )
                  )
                )
              )
            ) * 2
          ),

          /* -------- STATIC SCALARS -------- */
          if(
            data_words[j+1] IS NOT NULL,
            multiIf(
              ni_types[j+1] = 'bytes32',
                concat('0x', data_words[j+1]),

              ni_types[j+1] = 'address',
                concat('0x', substring(data_words[j+1], 25, 40)),

              startsWith(ni_types[j+1],'uint')
              OR ni_types[j+1] = 'bool',
                /* bool is stored as a 0/1 uint256 word — same decode path
                   as uint*. Output is '0' or '1' (decimal string). */
                toString(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))),

              startsWith(ni_types[j+1],'int'),
                toString(reinterpretAsInt256(reverse(unhex(data_words[j+1])))),

              NULL
            ),
            NULL
          )
        )
      ),
      range(length(ni_types))
    ) AS raw_values_str,

    -- Human-friendly normalization to STRING:
    -- - Arrays already JSON strings: pass through
    -- - Strings: hex → utf8 (remove NULs)
    -- - Bytes/bytesN: ensure 0x prefix
    arrayMap(j ->
      multiIf(
        endsWith(ni_types[j+1],'[]') AND raw_values_str[j+1] IS NOT NULL,
          raw_values_str[j+1],

        ni_types[j+1] = 'string' AND raw_values_str[j+1] IS NOT NULL,
          replaceRegexpAll(reinterpretAsString(unhex(raw_values_str[j+1])),'\0',''),

        ((ni_types[j+1] = 'bytes') OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'))
          AND raw_values_str[j+1] IS NOT NULL,
          concat('0x', raw_values_str[j+1]),

        /* else */
        raw_values_str[j+1]
      ),
      range(length(ni_types))
    ) AS decoded_ni_values,

    -- positions of indexed params (0-based positions into the param list)
    arrayMap(x -> x.3,
      arrayFilter((f,t,i) -> f, arrayZip(a.flags, a.types, range(n_params)))
    ) AS indexed_positions,

    -- stitch back into full order (correct topic index using 1-based indexOf)
    arrayMap(i ->
      if(
        param_flags[i+1],
        /* k1 is 1-based; 0 means not found */
        multiIf(
          indexOf(indexed_positions, i) = 0,
            NULL,
          param_types[i+1] = 'address',
            concat(
              '0x',
              substring(
                replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x',''),
                25, 40
              )
            ),
          startsWith(param_types[i+1],'uint')
          OR param_types[i+1] = 'bool',
            /* Indexed bool: same reinterpret path as uint*. Output '0'/'1'. */
            toString(
              reinterpretAsUInt256(
                reverse(
                  unhex(
                    replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x','')
                  )
                )
              )
            ),
          startsWith(param_types[i+1],'int'),
            toString(
              reinterpretAsInt256(
                reverse(
                  unhex(
                    replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x','')
                  )
                )
              )
            ),
          /* default: bytes32/topic hash as 0x + 64 hex chars */
          concat(
            '0x',
            substring(
              replaceAll(arrayElement(raw_topics, indexOf(indexed_positions, i)), '0x',''),
              1, 64
            )
          )
        ),
        /* non-indexed: pick correct decoded value */
        decoded_ni_values[indexOf(ni_positions, i)]
      ),
      range(n_params)
    ) AS param_values,

    -- final JSON or map (all values are full strings; arrays are JSON strings)
    
      mapFromArrays(param_names, param_values) AS decoded_params
    

  FROM logs_with_abi AS l
  ANY LEFT JOIN abi AS a
    ON replaceAll(l.topic0,'0x','') = a.topic0_sig
   AND l.abi_join_address = a.abi_contract_address
)

SELECT
  block_number,
  block_timestamp,
  transaction_hash,
  transaction_index,
  log_index,
  contract_address,
  event_name,
  decoded_params
FROM process

    )
    WHERE event_name IN ('SafeSetup','AddedOwner','RemovedOwner')
),

safe_setup_rows AS (
    -- One row per decoded owner. Guard: only setups whose owners array
    -- actually decoded (length > 0) reach the ARRAY JOIN — an empty array
    -- makes range(1,1) empty and the row would silently drop. Owner-less
    -- setups are re-added by safe_setup_ownerless_rows below.
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        'safe_setup'                                                AS event_kind,
        lower(JSONExtractString(d.decoded_params['owners'], idx))   AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])               AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SafeSetup' AND JSONLength(decoded_params['owners']) > 0) d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['owners'])) + 1) AS idx
),

safe_setup_ownerless_rows AS (
    -- Parity with the Dune spine: a SafeSetup whose owners array failed to
    -- decode still ISSUES the card, with a NULL owner, instead of vanishing
    -- from the wallet list entirely (observed at least once on Celo — a
    -- setup that needed a raw-byte owner fallback). action_value is Nullable
    -- downstream, so NULL is the honest "owner unknown" signal, not a
    -- silently dropped card. Keeps native issuance counts matching Dune.
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        'safe_setup'                                                AS event_kind,
        CAST(NULL AS Nullable(String))                              AS owner,
        toUInt32OrNull(d.decoded_params['threshold'])               AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM decoded d
    WHERE d.event_name = 'SafeSetup'
      AND coalesce(JSONLength(d.decoded_params['owners']), 0) = 0
),

owner_delta_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                     AS safe_address,
        if(d.event_name = 'AddedOwner', 'added_owner', 'removed_owner') AS event_kind,
        lower(d.decoded_params['owner'])                            AS owner,
        CAST(NULL AS Nullable(UInt32))                              AS threshold,
        d.block_timestamp                                           AS block_timestamp,
        d.block_number                                              AS block_number,
        concat('0x', d.transaction_hash)                            AS transaction_hash,
        d.log_index                                                 AS log_index
    FROM decoded d
    WHERE d.event_name IN ('AddedOwner','RemovedOwner')
)

SELECT * FROM safe_setup_rows
UNION ALL
SELECT * FROM safe_setup_ownerless_rows
UNION ALL
SELECT * FROM owner_delta_rows