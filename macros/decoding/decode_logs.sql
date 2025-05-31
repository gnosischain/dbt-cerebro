{# ================================================================
   decode_logs.sql - Decode EVM Contract Event Logs

   This macro decodes raw blockchain event logs into human-readable event
   parameters based on the contract’s ABI event signatures.

   Purpose:
   - Filters logs by contract address and topic0 signature
   - Loads event ABI: names, types, indexed flags
   - Separates indexed (topics) vs non-indexed (data) parameters
   - Applies head-/tail-style decoding only over the packed non-indexed data
   - Handles address extraction from topics, plus static/dynamic types
   - Assembles a Map or JSON string mapping parameter names to values

   Parameters:
   - source_table      : Source table containing the event logs
   - contract_address  : Ethereum contract address whose events to decode
   - output_json_type  : If true, returns a native Map; otherwise JSON string
   - incremental_column: Column used for incremental processing (e.g. block_timestamp)
   - address_column    : Column containing the log contract address (default: address)

   Requirements:
   - A source table raw_abi.event_signatures with topic0, names, types, flags
   - Supports ClickHouse Cloud with topic1–topic3 columns
   - ClickHouse 24.x+ (no external UDFs)

   Usage Example:
   {{
     decode_logs(
       source_table      = source('execution','logs'),
       contract_address  = '0xMyContract',
       output_json_type  = true,
       incremental_column= 'block_timestamp'
     )
   }}
================================================================ #}

{% macro decode_logs(
        source_table,
        contract_address,
        output_json_type=false,
        incremental_column='block_timestamp',
        address_column='address'
) %}

{# — normalize contract address — #}
{% set addr = contract_address | lower | replace('0x','') %}

{# — pull in the ABI for this contract — #}
{% set sig_sql %}
SELECT
  replace(signature,'0x','')                     AS topic0_sig,
  event_name,
  arrayMap(x->JSONExtractString(x,'name'),
           JSONExtractArrayRaw(params))          AS names,
  arrayMap(x->JSONExtractString(x,'type'),
           JSONExtractArrayRaw(params))          AS types,
  arrayMap(x->JSONExtractBool(x,'indexed'),
           JSONExtractArrayRaw(params))          AS flags
FROM {{ source('raw_abi','event_signatures') }}
WHERE replaceAll(lower(contract_address),'0x','') = '{{ addr }}'
{% endset %}

{% set sql_body %}
WITH

logs AS (
  SELECT *
  FROM {{ source_table }}
  WHERE replaceAll(lower({{ address_column }}),'0x','') = '{{ addr }}'
    {% if incremental_column and not flags.FULL_REFRESH %}
      AND {{ incremental_column }} >
        (SELECT coalesce(max({{ incremental_column }}),'1970-01-01')
         FROM {{ this }})
    {% endif %}
),

abi AS ( {{ sig_sql }} ),

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
            concat(
              '0x',
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
              )
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
    {% if output_json_type %}
      mapFromArrays(param_names, param_values) AS decoded_params
    {% else %}
      toJSONString(mapFromArrays(param_names, param_values)) AS decoded_params
    {% endif %}

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
{% endset %}

{{ sql_body | trim }}
{% endmacro %}
