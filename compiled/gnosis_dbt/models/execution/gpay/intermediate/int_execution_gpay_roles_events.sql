



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
    WHERE lower(replaceAll(address, '0x', '')) IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM `dbt`.`contracts_gpay_modules_registry` cw WHERE cw.contract_type = 'RolesModule')

      
        AND block_timestamp >= toDateTime('2023-06-01')
      

      
      

      
        AND block_timestamp >
          (SELECT coalesce(max(block_timestamp),'1970-01-01')
           FROM `dbt`.`int_execution_gpay_roles_events`)
      
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
     AND cw.contract_type = 'RolesModule'
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
WHERE replaceAll(lower(contract_address),'0x','') IN (SELECT lower(replaceAll(coalesce(nullIf(cw.abi_source_address, ''), cw.address), '0x', '')) FROM `dbt`.`contracts_gpay_modules_registry` cw WHERE cw.contract_type = 'RolesModule')
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
),

-- AssignRoles unrolled: one row per (member_module, role_key, is_member).
-- The decoded `module` topic1 is the address being granted/revoked the
-- roles — typically a spender delegate EOA.
--
-- The pre-filter subquery is necessary because ARRAY JOIN evaluates its
-- range expression BEFORE the outer WHERE clause runs. Without the
-- subquery, non-AssignRoles rows reach the ARRAY JOIN with NULL
-- decoded_params['roleKeys'], and `range(1, toUInt32(JSONLength(NULL)) + 1)`
-- fails with `Illegal (null) value`. Filtering inside the subquery means
-- the ARRAY JOIN only sees rows where roleKeys is guaranteed non-null.
assign_role_rows AS (
    SELECT
        concat('0x', lower(contract_address))                                AS roles_module_address,
        'AssignRoles'                                                        AS event_name,
        lower(decoded_params['module'])                                      AS member_address,
        JSONExtractString(decoded_params['roleKeys'], idx)                   AS role_key,
        toUInt8OrNull(JSONExtractString(decoded_params['memberOf'], idx))    AS is_member,
        CAST(NULL AS Nullable(String))                                       AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        block_timestamp,
        block_number,
        concat('0x', transaction_hash)                                       AS transaction_hash,
        log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'AssignRoles') d
    ARRAY JOIN range(1, toUInt32(JSONLength(d.decoded_params['roleKeys'])) + 1) AS idx
),

-- set_allowance_rows uses the same pre-filter subquery pattern as
-- assign_role_rows. The subquery is necessary because ClickHouse SELECT
-- aliases SHADOW source columns in WHERE clauses: writing
--   SELECT 'SetAllowance' AS event_name FROM decoded WHERE event_name = 'SetAllowance'
-- evaluates the WHERE against the alias literal ('SetAllowance' = 'SetAllowance'
-- is always TRUE), so every decoded row would pass through. Wrapping the
-- FROM in a subquery that selects `*` (no literal alias) ensures the WHERE
-- runs in a scope where nothing shadows `event_name`.
set_allowance_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                              AS roles_module_address,
        'SetAllowance'                                                       AS event_name,
        CAST(NULL AS Nullable(String))                                       AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        d.decoded_params['allowanceKey']                                     AS allowance_key,
        d.decoded_params['balance']                                          AS balance,
        d.decoded_params['maxRefill']                                        AS max_refill,
        d.decoded_params['refill']                                           AS refill,
        d.decoded_params['period']                                           AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                                     AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'SetAllowance') d
),

-- consume_allowance_rows uses the same pre-filter subquery pattern.
-- See the comment on set_allowance_rows above for why this is required.
consume_allowance_rows AS (
    SELECT
        concat('0x', lower(d.contract_address))                              AS roles_module_address,
        'ConsumeAllowance'                                                   AS event_name,
        CAST(NULL AS Nullable(String))                                       AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        d.decoded_params['allowanceKey']                                     AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        d.decoded_params['consumed']                                         AS consumed,
        d.decoded_params['newBalance']                                       AS new_balance,
        CAST(NULL AS Nullable(String))                                       AS default_role_key,
        d.block_timestamp,
        d.block_number,
        concat('0x', d.transaction_hash)                                     AS transaction_hash,
        d.log_index
    FROM (SELECT * FROM decoded WHERE event_name = 'ConsumeAllowance') d
),

setup_and_default_rows AS (
    SELECT
        concat('0x', lower(contract_address))                                AS roles_module_address,
        event_name,
        lower(decoded_params['module'])                                      AS member_address,
        CAST(NULL AS Nullable(String))                                       AS role_key,
        CAST(NULL AS Nullable(UInt8))                                        AS is_member,
        CAST(NULL AS Nullable(String))                                       AS allowance_key,
        CAST(NULL AS Nullable(String))                                       AS balance,
        CAST(NULL AS Nullable(String))                                       AS max_refill,
        CAST(NULL AS Nullable(String))                                       AS refill,
        CAST(NULL AS Nullable(String))                                       AS period,
        CAST(NULL AS Nullable(String))                                       AS consumed,
        CAST(NULL AS Nullable(String))                                       AS new_balance,
        decoded_params['defaultRoleKey']                                     AS default_role_key,
        block_timestamp,
        block_number,
        concat('0x', transaction_hash)                                       AS transaction_hash,
        log_index
    FROM decoded
    WHERE event_name IN ('RolesModSetup','SetDefaultRole')
)

SELECT * FROM assign_role_rows
UNION ALL
SELECT * FROM set_allowance_rows
UNION ALL
SELECT * FROM consume_allowance_rows
UNION ALL
SELECT * FROM setup_and_default_rows