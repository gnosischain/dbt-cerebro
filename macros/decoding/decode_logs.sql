{# ================================================================
   decode_logs.sql - Decode EVM Contract Event Logs

   This macro decodes raw blockchain event logs into human-readable event
   parameters based on the contracts ABI event signatures.

   Purpose:
   - Filters logs by contract address and topic0 signature
   - Loads event ABI: names, types, indexed flags
   - Separates indexed (topics) vs non-indexed (data) parameters
   - Applies head-/tail-style decoding only over the packed non-indexed data
   - Handles address extraction from topics, plus static/dynamic types
   - Converts string types from hex to readable text
   - Assembles a Map or JSON string mapping parameter names to values

   Parameters:
   - source_table      : Source table containing the event logs
   - contract_address  : Ethereum contract address (string) or list of addresses (array)
                        Single address: '0x...' 
                        Multiple addresses: ['0x...', '0x...', ...]
   - output_json_type  : If true, returns a native Map; otherwise JSON string
   - incremental_column: Column used for incremental processing (e.g. block_timestamp)
   - address_column    : Column containing the log contract address (default: address)
   - start_blocktime   : Optional start timestamp for filtering

   Supports:
     - static scalars: address, bytes32, uint*, int*
     - dynamic scalars: string, bytes, bytesN (N≠32)
     - arrays: address[], bytes32[], uint*/int*[]  (as JSON array strings)

   Design:
     - All branches of decoding return String (or NULL) to satisfy CH typing.
     - Arrays are decoded fully, then wrapped with toJSONString([...]).
     - Strings are UTF-8 decoded (nulls removed); bytes-like get 0x + full hex.
   Requirements:
   - A source table raw_abi.event_signatures with topic0, names, types, flags
   - Supports ClickHouse Cloud with topic1–topic3 columns
   - ClickHouse 24.x+ (no external UDFs)

   Usage Example (single address):
   {{
     decode_logs(
       source_table      = source('execution','logs'),
       contract_address  = '0xMyContract',
       output_json_type  = true,
       incremental_column= 'block_timestamp'
     )
   }}

   Usage Example (multiple addresses):
   {{
     decode_logs(
       source_table      = source('execution','logs'),
       contract_address  = ['0xContract1', '0xContract2', '0xContract3'],
       output_json_type  = true,
       incremental_column= 'block_timestamp'
     )
   }}

================================================================ #}

{% macro decode_logs(
        source_table,
        contract_address=null,
        contract_address_ref=null,
        contract_type_filter=null,
        output_json_type=false,
        incremental_column='block_timestamp',
        address_column='address',
        start_blocktime=null  
) %}

{# Check if using ref model (new way) or address list (old way) #}
{% if contract_address_ref %}
  {% if contract_type_filter %}
    {% set type_where = " WHERE cw.contract_type = '" ~ contract_type_filter ~ "'" %}
  {% else %}
    {% set type_where = "" %}
  {% endif %}
  {% set addr_filter = "lower(replaceAll(" ~ address_column ~ ", '0x', '')) IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM " ~ contract_address_ref ~ " cw" ~ type_where ~ ")" %}
  {% set abi_filter = "replaceAll(lower(contract_address),'0x','') IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM " ~ contract_address_ref ~ " cw" ~ type_where ~ ")" %}
{% else %}
  {# EXISTING: Original logic - works exactly as before #}
  {# — Normalize contract_address to list — #}
  {% if contract_address is string %}
    {% set addr_list = [contract_address] %}
  {% else %}
    {% set addr_list = contract_address %}
  {% endif %}

  {# — Normalize addresses (remove 0x, lowercase, trim) — #}
  {% set normalized = [] %}
  {% for addr in addr_list %}
    {% set normalized_addr = addr | lower | replace('0x', '') | trim %}
    {% set _ = normalized.append(normalized_addr) %}
  {% endfor %}

  {# — Build address filter for WHERE clause — #}
  {% if normalized | length > 1 %}
    {# Multiple addresses: use IN clause #}
    {% set addr_quoted = [] %}
    {% for addr in normalized %}
      {% set _ = addr_quoted.append("'" ~ addr ~ "'") %}
    {% endfor %}
    {% set addr_filter = "lower(replaceAll(" ~ address_column ~ ", '0x', '')) IN (" ~ addr_quoted | join(', ') ~ ")" %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') IN (" ~ addr_quoted | join(', ') ~ ")" %}
  {% else %}
    {# Single address: use equality (backward compatible) #}
    {% set addr = normalized[0] %}
    {% set addr_filter = address_column ~ " = '" ~ addr ~ "'" %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') = '" ~ addr ~ "'" %}
  {% endif %}
{% endif %}

{# — pull in the ABI for this contract(s) — #}
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
FROM {{ ref('event_signatures') }}
WHERE {{ abi_filter }}
{% endset %}

{% set sql_body %}
WITH

logs AS (
  SELECT *
  FROM {{ source_table }}
  WHERE {{ addr_filter }}
  
    {% if start_blocktime is not none and start_blocktime|trim != '' %}
      AND toStartOfMonth({{ incremental_column }}) >= toStartOfMonth(toDateTime('{{ start_blocktime }}'))
    {% endif %}

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

                startsWith(ni_base_types[j+1], 'uint') OR startsWith(ni_base_types[j+1], 'int'),
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

              startsWith(ni_types[j+1],'uint') OR startsWith(ni_types[j+1],'int'),
                toString(reinterpretAsUInt256(reverse(unhex(data_words[j+1])))),

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
          startsWith(param_types[i+1],'uint') OR startsWith(param_types[i+1],'int'),
            toString(
              reinterpretAsUInt256(
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
    {% if output_json_type %}
      mapFromArrays(param_names, param_values) AS decoded_params
    {% else %}
      toJSONString(mapFromArrays(param_names, param_values)) AS decoded_params
    {% endif %}

  FROM logs AS l
  ANY LEFT JOIN abi AS a
    --ON l.topic0 = concat('0x', a.topic0_sig)
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
