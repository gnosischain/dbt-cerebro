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
        selector_column='to_address'
) %}

{% set addr = contract_address | lower | replace('0x','') %}

{% set sig_sql %}
SELECT
    substring(signature,1,8) AS selector,
    function_name,
    arrayMap(x -> JSONExtractString(x, 'name'),
             arraySort(x -> toInt32OrZero(JSONExtractRaw(x, 'position')),
                       JSONExtractArrayRaw(input_params))) AS names,
    arrayMap(x -> JSONExtractString(x, 'type'),
             arraySort(x -> toInt32OrZero(JSONExtractRaw(x, 'position')),
                       JSONExtractArrayRaw(input_params))) AS types
FROM {{ ref('function_signatures') }}
WHERE replaceAll(lower(contract_address),'0x','') = '{{ addr }}'
{% endset %}

{% set sql_body %}
WITH
  tx AS (
    SELECT *
    FROM {{ tx_table }}
    WHERE replaceAll(lower({{ selector_column }}),'0x','') = '{{ addr }}'
      {% if incremental_column and not flags.FULL_REFRESH %}
        AND {{ incremental_column }} >
            (SELECT coalesce(max({{ incremental_column }}), '1970-01-01') FROM {{ this }})
      {% endif %}
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
      substring(replaceAll(t.input, '0x', ''),1,8) AS call_selector,
      substring(replaceAll(t.input, '0x', ''),9) AS args_raw_hex,
      a.names AS param_names,
      a.types AS param_types,

      -- flags for dynamic params
      arrayMap(i ->
        if(
          param_types[i+1] = 'bytes' OR
          param_types[i+1] = 'string' OR
          endsWith(param_types[i+1],'[]') OR
          (startsWith(param_types[i+1],'bytes') AND param_types[i+1] != 'bytes32'),
          1,
          0
        ),
        range(length(param_types))
      ) AS is_dynamic,

      -- decode each param
      arrayMap(i ->
        if(i < length(param_types),
          if(is_dynamic[i+1] = 1,
            -- dynamic: offset in head + length + data
            (
              if(
                toUInt32(reinterpretAsUInt256(reverse(unhex(substring(args_raw_hex,1+i*64,64))))) IS NOT NULL
                AND (toUInt32(reinterpretAsUInt256(reverse(unhex(substring(args_raw_hex,1+i*64,64)))))/32+1)*64 < length(args_raw_hex),
                concat(
                  '0x',
                  substring(
                    args_raw_hex,
                    1 + (toUInt32(reinterpretAsUInt256(reverse(unhex(substring(args_raw_hex,1+i*64,64)))))/32+1)*64,
                    toUInt32(
                      reinterpretAsUInt256(
                        reverse(unhex(
                          substring(
                            args_raw_hex,
                            1 + (toUInt32(reinterpretAsUInt256(reverse(unhex(substring(args_raw_hex,1+i*64,64)))))/32)*64,
                            64
                          )
                        ))
                      )
                    ) * 2
                  )
                ),
                NULL
              )
            ),
            -- static: bytes32, address, uint
            if(i*64 < length(args_raw_hex),
              multiIf(
                param_types[i+1] = 'bytes32',
                  concat('0x', substring(args_raw_hex,1+i*64,64)),
                param_types[i+1] = 'address',
                  concat('0x', substring(substring(args_raw_hex,1+i*64,64),25,40)),
                startsWith(param_types[i+1],'uint'),
                  toString(
                    reinterpretAsUInt256(
                      reverse(unhex(substring(args_raw_hex,1+i*64,64)))
                    )
                  ),
                NULL
              ),
              NULL
            )
          ),
          NULL
        ),
        range(length(param_types))
      ) AS param_values,

      -- output
      {% if output_json_type %}
        mapFromArrays(param_names,param_values) AS decoded_input
      {% else %}
        toJSONString(mapFromArrays(param_names,param_values)) AS decoded_input
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
