










WITH
  tx AS (
    SELECT *
    FROM `execution`.`transactions`
    WHERE replaceAll(lower(to_address),'0x','') = '0b98057ea310f4d31f2a452b414647007d1645d9'
      
        AND block_timestamp >
            (SELECT coalesce(max(block_timestamp), '1970-01-01') FROM `dbt`.`contracts_GBCDeposit_calls`)
      
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
WHERE replaceAll(lower(contract_address),'0x','') = '0b98057ea310f4d31f2a452b414647007d1645d9'
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
      
        mapFromArrays(param_names,param_values) AS decoded_input
      
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
