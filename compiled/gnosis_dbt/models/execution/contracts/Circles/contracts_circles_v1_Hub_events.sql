












WITH

logs AS (
  SELECT *
  FROM `execution`.`logs`
  WHERE replaceAll(lower(address),'0x','') = '29b9a7fbb8995b2423a71cc17cf9810798f6c543'
    
      AND block_timestamp >
        (SELECT coalesce(max(block_timestamp),'1970-01-01')
         FROM `dbt`.`contracts_circles_v1_Hub_events`)
    
),

abi AS ( 
SELECT
  replace(signature,'0x','')                     AS topic0_sig,
  event_name,
  arrayMap(x->JSONExtractString(x,'name'),
           JSONExtractArrayRaw(params))          AS names,
  arrayMap(x->JSONExtractString(x,'type'),
           JSONExtractArrayRaw(params))          AS types,
  arrayMap(x->JSONExtractBool(x,'indexed'),
           JSONExtractArrayRaw(params))          AS flags
FROM `dbt`.`event_signatures`
WHERE replaceAll(lower(contract_address),'0x','') = '29b9a7fbb8995b2423a71cc17cf9810798f6c543'
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

    -- non-indexed metadata
    arrayFilter((f,t,i) -> not f,
      arrayZip(a.flags, a.types, range(n_params))
    )                                    AS ni_meta,

    arrayMap(x -> x.2, ni_meta)           AS ni_types,
    arrayMap(x -> x.3, ni_meta)           AS ni_positions,

    -- split data into words for non-indexed params
    arrayMap(i ->
      if(i*64 < length(data_hex),
         substring(data_hex, 1 + i*64, 64),
         NULL),
      range(length(ni_types)*10)
    )                                    AS data_words,

    -- decode non-indexed values head/tail
    arrayMap(j ->
      if(
        -- dynamic types
        ni_types[j+1] = 'bytes'
        OR ni_types[j+1] = 'string'
        OR endsWith(ni_types[j+1],'[]')
        OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'),

        -- dynamic: extract offset, length, and data chunk
        (
          if(
            toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) IS NOT NULL
            AND (toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) / 32 + 1) * 64 < length(data_hex),
            
            -- Extract the raw hex data first
            substring(
              data_hex,
              1 + (toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) / 32 + 1) * 64,
              toUInt32(
                reinterpretAsUInt256(
                  reverse(unhex(
                    substring(
                      data_hex,
                      1 + (toUInt32(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))) / 32) * 64,
                      64
                    )
                  ))
                )
              ) * 2
            ),
            NULL
          )
        ),

        -- static types: bytes32, address, uint
        (
          if(
            data_words[j+1] IS NOT NULL,
            multiIf(
              ni_types[j+1] = 'bytes32',
                concat('0x', data_words[j+1]),

              ni_types[j+1] = 'address',
                concat(
                  '0x',
                  substring(data_words[j+1], 25, 40)
                ),

              startsWith(ni_types[j+1],'uint') OR startsWith(ni_types[j+1],'int'),
                toString(
                  reinterpretAsUInt256(
                    reverse(unhex(data_words[j+1]))
                  )
                ),

              NULL
            ),
            NULL
          )
        )
      ),
      range(length(ni_types))
    ) AS raw_decoded_values,

    -- Convert string types from hex to text
    arrayMap(j ->
      if(
        ni_types[j+1] = 'string' AND raw_decoded_values[j+1] IS NOT NULL,
        -- Convert hex to UTF-8 string, removing null bytes
        replaceRegexpAll(
          reinterpretAsString(unhex(raw_decoded_values[j+1])),
          '\0',
          ''
        ),
        -- For non-string types, keep the original value but add 0x prefix for bytes
        if(
          (ni_types[j+1] = 'bytes' OR (startsWith(ni_types[j+1],'bytes') AND ni_types[j+1] != 'bytes32'))
          AND raw_decoded_values[j+1] IS NOT NULL,
          concat('0x', raw_decoded_values[j+1]),
          raw_decoded_values[j+1]
        )
      ),
      range(length(ni_types))
    ) AS decoded_ni_values,

    -- stitch back into full order
    arrayMap(i ->
      if(
        param_flags[i+1],
        -- indexed: decode topic value
        multiIf(
          param_types[i+1] = 'address',
          concat(
            '0x',
            substring(
              replaceAll(raw_topics[i+1],'0x',''),
              25,
              40
            )
          ),
          startsWith(param_types[i+1],'uint') OR startsWith(param_types[i+1],'int'),
          toString(
                  reinterpretAsUInt256(
                    reverse(unhex(raw_topics[i+1]))
                  )
                ),
          concat('0x', substring(replaceAll(raw_topics[i+1],'0x',''),1,64))
        ),

        -- non-indexed: pick correct decoded value
        decoded_ni_values[
          indexOf(ni_positions, i)
        ]
      ),
      range(n_params)
    ) AS param_values,

    -- final JSON or map
    
      mapFromArrays(param_names, param_values) AS decoded_params
    

  FROM logs AS l
  ANY LEFT JOIN abi AS a
    ON replaceAll(l.topic0,'0x','') = a.topic0_sig
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
