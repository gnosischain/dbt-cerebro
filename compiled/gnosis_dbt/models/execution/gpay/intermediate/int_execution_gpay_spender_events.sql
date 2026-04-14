



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
    FROM `execution`.`logs`
    WHERE lower(replaceAll(address, '0x', '')) IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM `dbt`.`contracts_gpay_modules_registry` cw WHERE cw.contract_type = 'SpenderModule')

      
        AND block_timestamp >= toDateTime('2023-06-01')
      

      
      

      
        AND block_timestamp >
          (SELECT coalesce(max(block_timestamp),'1970-01-01')
           FROM `dbt`.`int_execution_gpay_spender_events`)
      
  )
  WHERE _dedup_rn = 1
),


logs_with_abi AS (
  SELECT
    l.*,
    
    lower(replaceAll(coalesce(nullIf(cw.abi_source_address, ''), cw.address), '0x', '')) AS abi_join_address
    
  FROM logs l
  ANY LEFT JOIN `dbt`.`contracts_gpay_modules_registry` cw
    ON lower(replaceAll(l.address, '0x', '')) = lower(replaceAll(cw.address, '0x', ''))
     AND cw.contract_type = 'SpenderModule'
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
WHERE replaceAll(lower(contract_address),'0x','') IN (SELECT lower(replaceAll(coalesce(nullIf(cw.abi_source_address, ''), cw.address), '0x', '')) FROM `dbt`.`contracts_gpay_modules_registry` cw WHERE cw.contract_type = 'SpenderModule')
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
)

SELECT
    concat('0x', lower(contract_address))                                   AS spender_module_address,
    event_name,

    -- Spend payload (only populated when event_name='Spend')
    lower(decoded_params['asset'])                                          AS spend_asset,
    lower(decoded_params['account'])                                        AS spend_account,
    lower(decoded_params['receiver'])                                       AS spend_receiver,
    decoded_params['amount']                                                AS spend_amount,

    -- AvatarSet payload
    lower(decoded_params['previousAvatar'])                                 AS previous_avatar,
    lower(decoded_params['newAvatar'])                                      AS new_avatar,

    -- TargetSet payload
    lower(decoded_params['previousTarget'])                                 AS previous_target,
    lower(decoded_params['newTarget'])                                      AS new_target,

    -- Module-management payload (Enabled/Disabled/ExecutionFromModule*)
    lower(decoded_params['module'])                                         AS module_address,

    -- OwnershipTransferred payload
    lower(decoded_params['previousOwner'])                                  AS previous_owner,
    lower(decoded_params['newOwner'])                                       AS new_owner,

    -- Initialized payload
    decoded_params['version']                                               AS init_version,

    block_timestamp,
    block_number,
    concat('0x', transaction_hash)                                          AS transaction_hash,
    log_index
FROM decoded
WHERE event_name IN (
    'Spend',
    'AvatarSet',
    'TargetSet',
    'EnabledModule',
    'DisabledModule',
    'ExecutionFromModuleSuccess',
    'ExecutionFromModuleFailure',
    'OwnershipTransferred',
    'Initialized',
    'HashExecuted',
    'HashInvalidated'
)