{# ================================================================
   decode_calls.sql - Decode Ethereum Contract Function Calls

   This macro decodes raw Ethereum transaction inputs for a specific smart
   contract address into human-readable function names and parameters.

   Purpose:
   - Extracts the 4-byte function selector from transaction input
   - Joins with an ABI table of function signatures to resolve names
   - Splits input into 32-byte words, handling both static and dynamic types
   - Decodes uint*, bytes32, address, and strings/bytes/arrays
   - Produces a ClickHouse Map or JSON string of parameter names to values

   Parameters:
   - tx_table          : Source table of transactions to decode
   - contract_address  : Ethereum contract address whose calls to decode
   - output_json_type  : If true, returns a native Map; otherwise JSON string
   - incremental_column: Column used for incremental processing (e.g. block_timestamp)
   - selector_column   : Column containing the to_address or method selector filter

   Requirements:
   - A source table raw_abi.function_signatures with selector, names, types
   - ClickHouse 24.x+ (no external UDFs)

   Usage Example:
   {{
     decode_calls(
       tx_table          = ref('ethereum_transactions'),
       contract_address  = '0xMyContract',
       output_json_type  = false,
       incremental_column= 'block_timestamp',
       selector_column   = 'to_address'
     )
   }}
================================================================ #}


{% macro decode_calls(
        tx_table,
        contract_address,
        output_json_type=false,
        incremental_column='block_timestamp',
        selector_column='to_address',
        start_blocktime=null
) %}

{% set addr = contract_address | lower | replace('0x','') %}

{% set sig_sql %}
SELECT
    substring(signature,1,8) AS selector,
    function_name,
    arraySort(x -> toInt32OrZero(JSONExtractRaw(x,'position')),
              ifNull(JSONExtractArrayRaw(input_params), emptyArrayString())) AS params_raw,
    arrayMap(x -> JSONExtractString(x,'name'), params_raw) AS names,
    arrayMap(x -> JSONExtractString(x,'type'), params_raw) AS types
FROM {{ ref('function_signatures') }}
WHERE replaceAll(lower(contract_address),'0x','') = '{{ addr }}'
{% endset %}

{% set sql_body %}
WITH
  tx AS (
    SELECT *
    FROM {{ tx_table }}
    WHERE replaceAll(lower({{ selector_column }}),'0x','') = '{{ addr }}'
      {% if start_blocktime %}
        AND toStartOfMonth({{ incremental_column }}) >= toStartOfMonth(toDateTime('{{ start_blocktime }}'))
      {% endif %}
      {% if incremental_column and not flags.FULL_REFRESH %}
        AND {{ incremental_column }} >
            (SELECT coalesce(max({{ incremental_column }}), '1970-01-01') FROM {{ this }})
      {% endif %}
      AND length(replaceAll(coalesce(input,''),'0x','')) >= 8
  ),

  abi AS ( {{ sig_sql }} ),

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

      arrayMap(i ->
        if(i*64 < length(args_raw_hex), substring(args_raw_hex, 1 + i*64, 64), NULL),
        range(greatest(length(param_types),1)*16)
      ) AS head_words,

      arrayMap(i -> replaceRegexpOne(param_types[i+1], '\\[\\]$', ''), range(length(param_types))) AS base_types,

      arrayMap(i ->
        if(i >= n_params, NULL,

          if(param_types[i+1] = 'tuple',
            toJSONString(
              mapFromArrays(
                arrayMap(c -> coalesce(JSONExtractString(c,'name'), ''),
                         ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                arrayMap(j ->
                  (
                    arrayElement(
                      arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                               ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                      j+1
                    )
                  ,
                    replaceRegexpOne(
                      arrayElement(
                        arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                 ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                        j+1
                      ),
                      '\\[\\]$',
                      ''
                    )
                  ,
                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2
                  ,
                    substring(
                      args_raw_hex,
                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                      64
                    )
                  ,
                    if(
                      endsWith(
                        arrayElement(
                          arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                   ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                          j+1
                        ),
                        '[]'
                      ),
                      toJSONString(
                        multiIf(
                          replaceRegexpOne(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            '\\[\\]$',
                            ''
                          ) = 'string',

                            arrayMap(k ->
                              replaceRegexpAll(
                                reinterpretAsString(unhex(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                          substring(
                                            args_raw_hex,
                                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                            64
                                          )
                                        )))) * 2,
                                        64
                                      )
                                    )))) * 2)
                                    + 64
                                    + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                          substring(
                                            args_raw_hex,
                                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                            64
                                          )
                                        )))) * 2),
                                        64
                                      )
                                    )))) * 2
                                    + k*64,
                                    toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                          substring(
                                            args_raw_hex,
                                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                            64
                                          )
                                        )))) * 2) + 64 + k*64,
                                        64
                                      )
                                    )))) * 2
                                  )
                                )),
                                '\0',''
                              ),
                              range(
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                ))))
                              )
                            ),

                          replaceRegexpOne(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            '\\[\\]$',
                            ''
                          ) = 'bytes',

                            arrayMap(k ->
                              concat(
                                '0x',
                                substring(
                                  args_raw_hex,
                                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                        substring(
                                          args_raw_hex,
                                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                          64
                                        )
                                      )))) * 2),
                                      64
                                    )
                                  )))) * 2)
                                  + 64
                                  + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                        substring(
                                          args_raw_hex,
                                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                          64
                                        )
                                      )))) * 2),
                                      64
                                    )
                                  )))) * 2
                                  + k*64,
                                  toUInt64(reinterpretAsUInt256(reverse(unhex(
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                        substring(
                                          args_raw_hex,
                                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                          64
                                        )
                                      )))) * 2) + 64 + k*64,
                                      64
                                    )
                                  )))) * 2
                                )
                              ),
                              range(
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                ))))
                              )
                            ),

                          replaceRegexpOne(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            '\\[\\]$',
                            ''
                          ) = 'address',

                            arrayMap(k ->
                              concat(
                                '0x',
                                substring(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2)) + 64 + k*64,
                                    64
                                  ),
                                  25, 40
                                )
                              ),
                              range(
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                ))))
                              )
                            ),

                          replaceRegexpOne(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            '\\[\\]$',
                            ''
                          ) = 'bytes32',

                            arrayMap(k ->
                              concat(
                                '0x',
                                substring(
                                  args_raw_hex,
                                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                      64
                                    )
                                  )))) * 2)) + 64 + k*64,
                                  64
                                )
                              ),
                              range(
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                ))))
                              )
                            ),

                          startsWith(
                            replaceRegexpOne(
                              arrayElement(
                                arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                         ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                                j+1
                              ),
                              '\\[\\]$',
                              ''
                            ),
                            'uint'
                          ) OR startsWith(
                            replaceRegexpOne(
                              arrayElement(
                                arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                         ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                                j+1
                              ),
                              '\\[\\]$',
                              ''
                            ),
                            'int'
                          ) OR startsWith(
                            replaceRegexpOne(
                              arrayElement(
                                arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                         ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                                j+1
                              ),
                              '\\[\\]$',
                              ''
                            ),
                            'bytes'
                          ),

                            arrayMap(k ->
                              multiIf(
                                startsWith(
                                  replaceRegexpOne(
                                    arrayElement(
                                      arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                               ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                                      j+1
                                    ),
                                    '\\[\\]$',
                                    ''
                                  ),
                                  'uint'
                                ) OR startsWith(
                                  replaceRegexpOne(
                                    arrayElement(
                                      arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                               ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                                      j+1
                                    ),
                                    '\\[\\]$',
                                    ''
                                  ),
                                  'int'
                                ),
                                  toString(
                                    reinterpretAsUInt256(
                                      reverse(
                                        unhex(
                                          substring(
                                            args_raw_hex,
                                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                              substring(
                                                args_raw_hex,
                                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                                64
                                              )
                                            )))) * 2)) + 64 + k*64,
                                            64
                                          )
                                        )
                                      )
                                    )
                                  ),
                                  concat(
                                    '0x',
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                        substring(
                                          args_raw_hex,
                                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                          64
                                        )
                                      )))) * 2)) + 64 + k*64,
                                      64
                                    )
                                  )
                                ),
                              range(
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                ))))
                              )
                            ),

                          []
                        )
                      ),

                      if(
                        (arrayElement(
                           arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                    ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                           j+1
                         ) = 'string') OR
                        (arrayElement(
                           arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                    ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                           j+1
                         ) = 'bytes') OR
                        (startsWith(arrayElement(
                           arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                    ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                           j+1
                         ), 'bytes') AND arrayElement(
                           arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                    ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                           j+1
                         ) != 'bytes32'),

                        if(
                          arrayElement(
                            arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                     ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                            j+1
                          ) = 'string',
                          replaceRegexpAll(
                            reinterpretAsString(unhex(
                              substring(
                                args_raw_hex,
                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                    64
                                  )
                                )))) * 2)) + 64,
                                toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                      substring(
                                        args_raw_hex,
                                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                        64
                                      )
                                    )))) * 2),
                                    64
                                  )
                                )))) * 2
                              )
                            )),
                            '\0',''
                          ),
                          concat(
                            '0x',
                            substring(
                              args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                substring(
                                  args_raw_hex,
                                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                  64
                                )
                              )))) * 2)) + 64,
                              toUInt64(reinterpretAsUInt256(reverse(unhex(
                                substring(
                                  args_raw_hex,
                                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                    substring(
                                      args_raw_hex,
                                      (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                      64
                                    )
                                  )))) * 2),
                                  64
                                )
                              )))) * 2
                            )
                          )
                        ),

                        multiIf(
                          arrayElement(
                            arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                     ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                            j+1
                          ) = 'bytes32',
                            concat('0x', substring(
                              args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                              64
                            )),
                          arrayElement(
                            arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                     ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                            j+1
                          ) = 'address',
                            concat('0x', substring(
                              substring(
                                args_raw_hex,
                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                64
                              ),
                              25, 40
                            )),
                          startsWith(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            'uint'
                          ) OR startsWith(
                            arrayElement(
                              arrayMap(c -> coalesce(JSONExtractString(c,'type'), ''),
                                       ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())),
                              j+1
                            ),
                            'int'
                          ),
                            toString(
                              reinterpretAsUInt256(
                                reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                                    64
                                  )
                                ))
                              )
                            ),
                          concat('0x', substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + j*64,
                            64
                          ))
                        )
                      )
                    )
                  ,
                    range(
                      length(
                        ifNull(JSONExtractArrayRaw(arrayElement(param_objs,i+1),'components'), emptyArrayString())
                      )
                    )
                  )
                )
              ),
            if(endsWith(param_types[i+1],'[]'),
              toJSONString(
                multiIf(
                  base_types[i+1] = 'string',
                    arrayMap(k ->
                      replaceRegexpAll(
                        reinterpretAsString(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2)
                              + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                  substring(
                                    args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                                  )
                                )))) * 2
                              + 64
                              + k*64,
                            toUInt64(reinterpretAsUInt256(reverse(unhex(
                              substring(
                                args_raw_hex,
                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64, 64
                              )
                            )))) * 2
                          )
                        )),
                        '\0',''
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                          )
                        ))))
                      )
                    ),
                  base_types[i+1] = 'bytes',
                    arrayMap(k ->
                      concat('0x',
                        substring(
                          args_raw_hex,
                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2)
                            + toUInt64(reinterpretAsUInt256(reverse(unhex(
                                substring(
                                  args_raw_hex,
                                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                                )
                              )))) * 2
                            + 64
                            + k*64,
                          toUInt64(reinterpretAsUInt256(reverse(unhex(
                            substring(
                              args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64, 64
                            )
                          )))) * 2
                        )
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                          )
                        ))))
                      )
                    ),
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
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                          )
                        ))))
                      )
                    ),
                  base_types[i+1] = 'bytes32',
                    arrayMap(k ->
                      concat('0x',
                        substring(
                          args_raw_hex,
                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                          64
                        )
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                          )
                        ))))
                      )
                    ),
                  startsWith(base_types[i+1],'uint') OR startsWith(base_types[i+1],'int') OR startsWith(base_types[i+1],'bytes'),
                    arrayMap(k ->
                      multiIf(
                        startsWith(base_types[i+1],'uint') OR startsWith(base_types[i+1],'int'),
                          toString(reinterpretAsUInt256(reverse(unhex(
                            substring(
                              args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                              64
                            )
                          )))),
                        concat('0x',
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                            64
                          )
                        )
                      ),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(
                            args_raw_hex,
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64
                          )
                        ))))
                      )
                    ),
                  []
                )
              ),
              if(
                (param_types[i+1] = 'bytes') OR (param_types[i+1] = 'string') OR (startsWith(param_types[i+1],'bytes') AND param_types[i+1] != 'bytes32'),
                substring(
                  args_raw_hex,
                  (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64,
                  toUInt64(reinterpretAsUInt256(reverse(unhex(
                    substring(args_raw_hex,
                              (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2), 64)
                  )))) * 2
                ),
                if(arrayElement(head_words,i+1) IS NULL, NULL,
                  multiIf(
                    param_types[i+1] = 'bytes32',
                      concat('0x', arrayElement(head_words,i+1)),
                    param_types[i+1] = 'address',
                      concat('0x', substring(arrayElement(head_words,i+1), 25, 40)),
                    startsWith(param_types[i+1],'uint') OR startsWith(param_types[i+1],'int'),
                      toString(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))),
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
        multiIf(
          i < n_params AND (param_types[i+1] = 'tuple' OR endsWith(param_types[i+1],'[]')) AND raw_values_str[i+1] IS NOT NULL,
            raw_values_str[i+1],
          i < n_params AND param_types[i+1] = 'string' AND raw_values_str[i+1] IS NOT NULL,
            replaceRegexpAll(reinterpretAsString(unhex(raw_values_str[i+1])),'\0',''),
          i < n_params AND (param_types[i+1] = 'bytes' OR (startsWith(param_types[i+1],'bytes') AND param_types[i+1] != 'bytes32')) AND raw_values_str[i+1] IS NOT NULL,
            concat('0x', raw_values_str[i+1]),
          raw_values_str[i+1]
        ),
        range(n_params)
      ) AS param_values_str,

      {% if output_json_type %}
        mapFromArrays(param_names, param_values_str) AS decoded_input
      {% else %}
        toJSONString(mapFromArrays(param_names, param_values_str)) AS decoded_input
      {% endif %}

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
{% endset %}

{{ sql_body | trim }}
{% endmacro %}
