











WITH
  tx AS (
    SELECT *
    FROM `execution`.`transactions`
    WHERE replaceAll(lower(to_address),'0x','') = 'e91d153e0b41518a2ce8dd3d7944fa863463a97d'

      
      
     
      
        AND block_timestamp >
            (SELECT coalesce(max(block_timestamp), '1970-01-01') FROM `dbt`.`contracts_wxdai_calls`)
      
  ),

  abi AS ( 
SELECT
    substring(signature,1,8) AS selector,
    function_name,
    arrayMap(x -> JSONExtractString(x, 'name'),
             arraySort(x -> toInt32OrZero(JSONExtractRaw(x, 'position')),
                       JSONExtractArrayRaw(input_params))) AS names,
    arrayMap(x -> JSONExtractString(x, 'type'),
             arraySort(x -> toInt32OrZero(JSONExtractRaw(x, 'position')),
                       JSONExtractArrayRaw(input_params))) AS types
FROM `dbt`.`function_signatures`
WHERE replaceAll(lower(contract_address),'0x','') = 'e91d153e0b41518a2ce8dd3d7944fa863463a97d'
 ),

  process AS (
    SELECT
      t.block_number,
      t.block_timestamp,
      t.transaction_hash,
      t.nonce,
      t.gas_price,
      t.value_string AS value,

      a.function_name,
      substring(replaceAll(t.input, '0x', ''), 1, 8)  AS call_selector,
      substring(replaceAll(t.input, '0x', ''), 9)     AS args_raw_hex,

      a.names AS param_names,
      a.types AS param_types,

      -- base types for arrays (strip trailing [])
      arrayMap(i -> replaceRegexpOne(param_types[i+1], '\\[\\]$', ''), range(length(param_types))) AS base_types,

      -- head words (32-byte) from start of args area (after selector)
      arrayMap(i ->
        if(i*64 < length(args_raw_hex),
           substring(args_raw_hex, 1 + i*64, 64),
           NULL),
        range(greatest(length(param_types), 1) * 16)  -- generous bound
      ) AS head_words,

      /* ===================== DECODING ======================
         For each param i produce a STRING:
         - Arrays: decode fully -> toJSONString(Array(String))
         - Dynamic scalars (string/bytes/bytesN≠32): payload hex (string step handles UTF-8)
         - Static scalars: decode directly from head
      ===================================================== */
      arrayMap(i ->
        if(
          i < length(param_types),

          -- -------- ARRAY TYPES --------
          if(
            endsWith(param_types[i+1], '[]'),
            toJSONString(
              arrayMap(
                k ->
                  multiIf(
                    base_types[i+1] = 'address',
                      concat(
                        '0x',
                        substring(
                          substring(
                            args_raw_hex,
                            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2 + 64 + k*64,
                            64
                          ),
                          25, 40
                        )
                      ),

                    base_types[i+1] = 'bytes32',
                      concat(
                        '0x',
                        substring(
                          args_raw_hex,
                          1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2 + 64 + k*64,
                          64
                        )
                      ),

                    startsWith(base_types[i+1], 'uint') OR startsWith(base_types[i+1], 'int'),
                      toString(
                        reinterpretAsUInt256(
                          reverse(
                            unhex(
                              substring(
                                args_raw_hex,
                                1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2 + 64 + k*64,
                                64
                              )
                            )
                          )
                        )
                      ),

                    -- Fallback: 32-byte hex
                    concat(
                      '0x',
                      substring(
                        args_raw_hex,
                        1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2 + 64 + k*64,
                        64
                      )
                    )
                  ),
                -- range(N): N is array length at offset base
                range(
                  toUInt32(
                    reinterpretAsUInt256(
                      reverse(
                        unhex(
                          substring(
                            args_raw_hex,
                            1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2,
                            64
                          )
                        )
                      )
                    )
                  )
                )
              )
            ),

            -- -------- DYNAMIC SCALARS (string/bytes/bytesN≠32) --------
            if(
              param_types[i+1] = 'bytes'
              OR param_types[i+1] = 'string'
              OR (startsWith(param_types[i+1],'bytes') AND param_types[i+1] != 'bytes32'),

              -- payload hex of exactly len bytes (converted to utf8 later for 'string')
              substring(
                args_raw_hex,
                1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2 + 64,
                toUInt32(
                  reinterpretAsUInt256(
                    reverse(
                      unhex(
                        substring(
                          args_raw_hex,
                          1 + toUInt32(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))) * 2,
                          64
                        )
                      )
                    )
                  )
                ) * 2
              ),

              -- -------- STATIC SCALARS --------
              if(
                head_words[i+1] IS NOT NULL,
                multiIf(
                  param_types[i+1] = 'bytes32',
                    concat('0x', head_words[i+1]),

                  param_types[i+1] = 'address',
                    concat('0x', substring(head_words[i+1], 25, 40)),

                  startsWith(param_types[i+1], 'uint') OR startsWith(param_types[i+1], 'int'),
                    toString(reinterpretAsUInt256(reverse(unhex(head_words[i+1])))),

                  NULL
                ),
                NULL
              )
            )
          ),
          NULL
        ),
        range(length(param_types))
      ) AS raw_values_str,

      -- Human-friendly normalization to STRING:
      -- - Arrays already JSON strings: pass through
      -- - Strings: hex → utf8 (remove NULs)
      -- - Bytes/bytesN: ensure 0x prefix
      arrayMap(i ->
        multiIf(
          i < length(param_types) AND endsWith(param_types[i+1],'[]') AND raw_values_str[i+1] IS NOT NULL,
            raw_values_str[i+1],

          i < length(param_types) AND param_types[i+1] = 'string' AND raw_values_str[i+1] IS NOT NULL,
            replaceRegexpAll(reinterpretAsString(unhex(raw_values_str[i+1])),'\0',''),

          i < length(param_types)
            AND (param_types[i+1] = 'bytes' OR (startsWith(param_types[i+1],'bytes') AND param_types[i+1] != 'bytes32'))
            AND raw_values_str[i+1] IS NOT NULL,
            concat('0x', raw_values_str[i+1]),

          /* else */
          raw_values_str[i+1]
        ),
        range(length(param_types))
      ) AS param_values_str,

      
        mapFromArrays(param_names, param_values_str) AS decoded_input
      

    FROM tx AS t
    ANY LEFT JOIN abi AS a
      ON substring(replaceAll(t.input,'0x',''),1,8) = a.selector
  )

SELECT
  block_number,
  block_timestamp,
  transaction_hash,
  nonce,
  gas_price,
  value,
  function_name,
  decoded_input
FROM process
