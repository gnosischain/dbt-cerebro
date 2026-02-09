










WITH
  tx AS (
    SELECT * FROM (
      SELECT *,
        row_number() OVER (
          PARTITION BY block_number, transaction_index
          ORDER BY insert_version DESC
        ) AS _dedup_rn
      FROM `execution`.`transactions`
      WHERE replaceAll(lower(to_address),'0x','') = '79e32ae03fb27b07c89c0c568f80287c01ca2e57'
        
          AND block_timestamp >= toDateTime('2021-01-13')
        

        
        

        
          AND block_timestamp >
              (SELECT coalesce(max(block_timestamp), '1970-01-01') FROM `dbt`.`contracts_Realitio_v2_1_calls`)
        
        AND length(replaceAll(coalesce(input,''),'0x','')) >= 8
    )
    WHERE _dedup_rn = 1
  ),
  abi AS ( 
SELECT
    substring(signature,1,8) AS selector,
    function_name,
    arraySort(x -> toInt32OrZero(JSONExtractRaw(x,'position')),
              ifNull(JSONExtractArrayRaw(input_params), emptyArrayString())) AS params_raw,
    arrayMap(x -> JSONExtractString(x,'name'), params_raw) AS names,
    arrayMap(x -> JSONExtractString(x,'type'), params_raw) AS types
FROM `dbt`.`function_signatures`
WHERE replaceAll(lower(contract_address),'0x','') = '79e32ae03fb27b07c89c0c568f80287c01ca2e57'
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
      substring(replaceAll(t.input,'0x',''),1,8) AS call_selector,
      substring(replaceAll(t.input,'0x',''),9)   AS args_raw_hex,
      a.names      AS param_names,
      a.types      AS param_types,
      a.params_raw AS param_objs,
      length(a.types) AS n_params,

      arrayMap(i -> if(i*64 < length(args_raw_hex), substring(args_raw_hex, 1 + i*64, 64), NULL),
               range(greatest(length(param_types),1)*16)) AS head_words,

      arrayMap(i -> replaceRegexpOne(param_types[i+1], '\\[\\]$', ''), range(length(param_types))) AS base_types,

      arrayMap(i ->
        if(i >= n_params, NULL,
          if(param_types[i+1] = 'tuple',
            toJSONString(
              mapFromArrays(
                arrayMap(c -> coalesce(JSONExtractString(c,'name'),''),
                         ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                arrayMap(j ->
                  if(
                    endsWith(
                      arrayElement(
                        arrayMap(c -> coalesce(JSONExtractString(c,'type'),''),
                                 ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                        j+1
                      ),
                      '[]'
                    ),
                    toJSONString([]),
                    if(
                      arrayElement(
                        arrayMap(c -> coalesce(JSONExtractString(c,'type'),''),
                                 ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                        j+1
                      ) = 'address',
                      concat('0x',
                        substring(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                            64
                          ),
                          25, 40
                        )
                      ),
                      concat('0x',
                        substring(
                          args_raw_hex,
                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                          64
                        )
                      )
                    )
                  ),
                  range(
                    length(ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString()))
                  )
                )
              )
            ),
            if(endsWith(param_types[i+1],'[]'),
              toJSONString(
                if(
                  base_types[i+1] = 'string',
                    arrayMap(k ->
                      replaceRegexpAll(
                        reinterpretAsString(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2)
                              + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(args_raw_hex,
                                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                                            64)
                              )))) * 2
                              + 64,
                            toUInt64(reinterpretAsUInt256(reverse(unhex(
                              substring(args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2)
                                          + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                              substring(args_raw_hex,
                                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                                                        64)
                                          )))) * 2,
                                        64)
                            )))) * 2
                          )
                        )),
                        '\0',''
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2),
                                    64)
                        ))))
                      )
                    ),
                  if(
                    base_types[i+1] = 'address',
                    arrayMap(k ->
                      concat('0x',
                        substring(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                            64
                          ),
                          25, 40
                        )
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2),
                                    64)
                        ))))
                      )
                    ),
                    []
                  )
                )
              ),
              if(
                (param_types[i+1] = 'bytes') OR (param_types[i+1] = 'string'),
                substring(
                  args_raw_hex,
                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64,
                  toUInt64(reinterpretAsUInt256(reverse(unhex(
                    substring(args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64)
                  )))) * 2
                ),
                if(arrayElement(head_words,i+1) IS NULL, NULL,
                  if(
                    param_types[i+1] = 'address',
                      concat('0x', substring(arrayElement(head_words,i+1), 25, 40)),
                      concat('0x', arrayElement(head_words,i+1))
                  )
                )
              )
            )
          )
        ),
        range(n_params)
      ) AS raw_values_str,

      arrayMap(i ->
        if(
          i < n_params AND param_types[i+1] = 'string' AND raw_values_str[i+1] IS NOT NULL,
            replaceRegexpAll(reinterpretAsString(unhex(raw_values_str[i+1])),'\0',''),
          if(
            i < n_params AND param_types[i+1] = 'bytes' AND raw_values_str[i+1] IS NOT NULL,
            concat('0x', raw_values_str[i+1]),
            raw_values_str[i+1]
          )
        ),
        range(n_params)
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
