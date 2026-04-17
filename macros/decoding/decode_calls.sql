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

{% macro decode_array_component(args_raw_hex, head_words, param_objs, i, j, component_type) %}
  {% if component_type == 'string' %}
    arrayMap(k ->
      replaceRegexpAll(
        reinterpretAsString(unhex(
          substring(
            {{ args_raw_hex }},
            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2)
              + toUInt64(reinterpretAsUInt256(reverse(unhex(
                  substring(
                    {{ args_raw_hex }},
                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                      substring({{ args_raw_hex }},
                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                                64)))
                    ))) * 2),
                    64
                  )
                )))) * 2
              + 64
              + toUInt64(reinterpretAsUInt256(reverse(unhex(
                  substring(
                    {{ args_raw_hex }},
                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                      substring({{ args_raw_hex }},
                                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                                64)))
                    ))) * 2) + 64 + k*64,
                    64
                  )
                )))) * 2,
            toUInt64(reinterpretAsUInt256(reverse(unhex(
              substring(
                {{ args_raw_hex }},
                (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                  substring({{ args_raw_hex }},
                            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                            64)))
                ))) * 2)
                  + toUInt64(reinterpretAsUInt256(reverse(unhex(
                      substring(
                        {{ args_raw_hex }},
                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring({{ args_raw_hex }},
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                                    64)))
                        ))) * 2) + 64 + k*64,
                        64
                      )
                    )))) * 2,
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
            {{ args_raw_hex }},
            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
              substring({{ args_raw_hex }},
                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                        64)))
            ))) * 2),
            64
          )
        ))))
      )
    )
  {% elif component_type == 'address' %}
    arrayMap(k ->
      concat(
        '0x',
        substring(
          substring(
            {{ args_raw_hex }},
            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
              substring({{ args_raw_hex }},
                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                        64)))
            ))) * 2) + 64 + k*64,
            64
          ),
          25, 40
        )
      ),
      range(
        toUInt64(reinterpretAsUInt256(reverse(unhex(
          substring(
            {{ args_raw_hex }},
            (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(
              substring({{ args_raw_hex }},
                        (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement({{ head_words }},{{ i }}+1))))) * 2) + {{ j }}*64,
                        64)))
            ))) * 2),
            64
          )
        ))))
      )
    )
  {% else %}
    []
  {% endif %}
{% endmacro %}

{% macro decode_calls(
        tx_table,
        contract_address=null,
        contract_address_ref=null,
        contract_type_filter=null,
        abi_source_address=null,
        output_json_type=false,
        incremental_column='block_timestamp',
        selector_column='to_address',
        start_blocktime=null
) %}

{% if contract_address_ref %}
  {% if contract_type_filter %}
    {% set type_where = " WHERE cw.contract_type = '" ~ contract_type_filter ~ "'" %}
    {% set type_and = " AND cw.contract_type = '" ~ contract_type_filter ~ "'" %}
  {% else %}
    {% set type_where = "" %}
    {% set type_and = "" %}
  {% endif %}

  {# ----------------------------------------------------------------
     Detect whether the referenced whitelist/registry seed exposes an
     `abi_source_address` column. See decode_logs.sql for the full
     rationale — short version: proxy registries (e.g.
     `contracts_circles_registry`) have this column for ABI override,
     simple flat whitelists (e.g. `contracts_whitelist`) do not.
     When the column is missing, fall back to using `cw.address`
     directly so the JOIN and subquery references stay valid.
     ---------------------------------------------------------------- #}
  {% set has_abi_source_col = false %}
  {% if execute %}
    {% set _cw_columns = adapter.get_columns_in_relation(contract_address_ref) %}
    {% set _cw_column_names = _cw_columns | map(attribute='name') | map('lower') | list %}
    {% if 'abi_source_address' in _cw_column_names %}
      {% set has_abi_source_col = true %}
    {% endif %}
  {% endif %}

  {% set addr_filter = "lower(replaceAll(" ~ selector_column ~ ",'0x','')) IN (SELECT lower(replaceAll(cw.address,'0x','')) FROM " ~ contract_address_ref ~ " cw" ~ type_where ~ ")" %}
  {% if abi_source_address %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') = '" ~ (abi_source_address | lower | replace('0x', '') | trim) ~ "'" %}
  {% elif has_abi_source_col %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') IN (SELECT lower(replaceAll(coalesce(nullIf(cw.abi_source_address, ''), cw.address), '0x', '')) FROM " ~ contract_address_ref ~ " cw" ~ type_where ~ ")" %}
  {% else %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') IN (SELECT lower(replaceAll(cw.address, '0x', '')) FROM " ~ contract_address_ref ~ " cw" ~ type_where ~ ")" %}
  {% endif %}
{% else %}
  {% if contract_address is string %}
    {% set addr_list = [contract_address] %}
  {% else %}
    {% set addr_list = contract_address %}
  {% endif %}

  {% set normalized = [] %}
  {% for addr in addr_list %}
    {% set normalized_addr = addr | lower | replace('0x', '') | trim %}
    {% set _ = normalized.append(normalized_addr) %}
  {% endfor %}

  {% if normalized | length > 1 %}
    {% set addr_quoted = [] %}
    {% for addr in normalized %}
      {% set _ = addr_quoted.append("'" ~ addr ~ "'") %}
    {% endfor %}
    {% set addr_filter = "lower(replaceAll(" ~ selector_column ~ ", '0x', '')) IN (" ~ addr_quoted | join(', ') ~ ")" %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') IN (" ~ addr_quoted | join(', ') ~ ")" %}
  {% else %}
    {% set addr = normalized[0] %}
    {% set addr_filter = "replaceAll(lower(" ~ selector_column ~ "),'0x','') = '" ~ addr ~ "'" %}
    {% set abi_filter = "replaceAll(lower(contract_address),'0x','') = '" ~ addr ~ "'" %}
  {% endif %}
{% endif %}

{% set sig_sql %}
SELECT
    replaceAll(lower(contract_address), '0x', '') AS abi_contract_address,
    substring(signature,1,8) AS selector,
    function_name,
    arraySort(x -> toInt32OrZero(JSONExtractRaw(x,'position')),
              ifNull(JSONExtractArrayRaw(input_params), emptyArrayString())) AS params_raw,
    arrayMap(x -> JSONExtractString(x,'name'), params_raw) AS names,
    arrayMap(x -> JSONExtractString(x,'type'), params_raw) AS types
FROM {{ ref('function_signatures') }}
WHERE {{ abi_filter }}
{% endset %}

{% set sql_body %}
{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{# ---------------------------------------------------------------
   Auto-detect whether `tx_table` points at `execution.traces`. When
   it does, we normalise the trace column names (action_input / action_to
   / etc.) onto the `execution.transactions` shape so the rest of the
   macro stays unchanged. Also emit a `trace_address` column in that
   case — both for the dedup key (one tx can have many internal calls
   to the same target) and for the final output.

   For traces mode we also pre-filter to successful real calls
   (action_call_type IN (...) AND error IS NULL) so the downstream
   abi-join logic sees a clean stream.

   Safety: existing transactions-mode callers get byte-identical
   output — no new column, no new filter, no schema drift on their
   incremental tables.
   --------------------------------------------------------------- #}
{% set tx_table_name = (tx_table | string) | lower %}
{% set is_traces = '"traces"' in tx_table_name or tx_table_name.endswith('.traces') or tx_table_name.endswith('traces`') %}

WITH
  src AS (
    {% if is_traces %}
    SELECT
      block_number,
      block_timestamp,
      transaction_hash,
      transaction_index,
      insert_version,
      action_input                         AS input,
      action_to                            AS to_address,
      action_from                          AS from_address,
      action_value                         AS value_string,
      CAST(NULL AS Nullable(UInt64))       AS nonce,
      CAST(NULL AS Nullable(UInt64))       AS gas_price,
      trace_address                        AS trace_address
    FROM {{ tx_table }}
    WHERE action_call_type IN ('call','delegate_call','static_call')
      AND error IS NULL
    {% else %}
    -- `insert_version` is MATERIALIZED on execution.transactions so it's NOT
    -- picked up by `SELECT *`. Reference it explicitly so the dedup
    -- ROW_NUMBER further down can see it. The previous implementation
    -- dodged this by reading `{{ tx_table }}` directly, which allows
    -- referencing MATERIALIZED columns alongside `*`. Same trick here.
    SELECT *, insert_version FROM {{ tx_table }}
    {% endif %}
  ),
  tx AS (
    SELECT * FROM (
      SELECT *,
        row_number() OVER (
          PARTITION BY block_number, transaction_index{% if is_traces %}, trace_address{% endif %}
          ORDER BY insert_version DESC
        ) AS _dedup_rn
      FROM src
      WHERE {{ addr_filter }}
        {% if start_blocktime %}
          AND {{ incremental_column }} >= toDateTime('{{ start_blocktime }}')
        {% endif %}

        {# Batch window filter: used by refresh.py for batched full refresh #}
        {% if start_month is not none and end_month is not none %}
          AND {{ incremental_column }} >= toDateTime('{{ start_month }}')
          AND {{ incremental_column }} <  toDateTime('{{ end_month }}') + INTERVAL 1 MONTH
        {% endif %}

        {% if incremental_column and not flags.FULL_REFRESH %}
          AND {{ incremental_column }} >
              (SELECT coalesce(max({{ incremental_column }}), '1970-01-01') FROM {{ this }})
        {% endif %}
        AND length(replaceAll(coalesce(input,''),'0x','')) >= 8
    )
    WHERE _dedup_rn = 1
  ),
  {% if contract_address_ref %}
  tx_with_abi AS (
    SELECT
      t.*,
      {% if abi_source_address %}
      '{{ abi_source_address | lower | replace('0x', '') | trim }}' AS abi_join_address
      {% elif has_abi_source_col %}
      lower(replaceAll(coalesce(nullIf(cw.abi_source_address, ''), cw.address), '0x', '')) AS abi_join_address
      {% else %}
      lower(replaceAll(cw.address, '0x', '')) AS abi_join_address
      {% endif %}
    FROM tx t
    ANY LEFT JOIN {{ contract_address_ref }} cw
      ON lower(replaceAll(t.{{ selector_column }}, '0x', '')) = lower(replaceAll(cw.address, '0x', ''))
      {{ type_and }}
  ),
  {% else %}
  tx_with_abi AS (
    SELECT
      t.*,
      lower(replaceAll(t.{{ selector_column }}, '0x', '')) AS abi_join_address
    FROM tx t
  ),
  {% endif %}
  abi AS ( {{ sig_sql }} ),

  process AS (
    SELECT
      t.block_number,
      t.block_timestamp,
      t.transaction_hash,
      t.{{ selector_column }} AS contract_address,
      t.nonce,
      t.gas_price,
      t.value_string AS value,
      {% if is_traces %}t.trace_address AS trace_address,{% endif %}
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
                  multiIf(
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
                          substring(args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2),
                                    64)
                        ))))
                      )
                    ),
                    /* bool[] shares the uint decode path: each element is a
                       0/1 uint256 word. Output '0'/'1' strings inside the
                       JSON array. */
                    startsWith(base_types[i+1], 'uint')
                    OR base_types[i+1] = 'bool',
                    arrayMap(k ->
                      toString(reinterpretAsUInt256(reverse(unhex(
                        substring(
                          args_raw_hex,
                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                          64
                        )
                      )))),
                      range(
                        toUInt64(reinterpretAsUInt256(reverse(unhex(
                          substring(args_raw_hex,
                                    (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2),
                                    64)
                        ))))
                      )
                    ),
                    startsWith(base_types[i+1], 'int'),
                    arrayMap(k ->
                      toString(reinterpretAsInt256(reverse(unhex(
                        substring(
                          args_raw_hex,
                          (1 + toUInt64(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))) * 2) + 64 + k*64,
                          64
                        )
                      )))),
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
                  multiIf(
                    param_types[i+1] = 'address',
                      concat('0x', substring(arrayElement(head_words,i+1), 25, 40)),
                    /* bool is stored as a 0/1 uint256 word. Output '0'/'1'
                       for consistency with decode_logs and with uint*. */
                    startsWith(param_types[i+1], 'uint')
                    OR param_types[i+1] = 'bool',
                      toString(reinterpretAsUInt256(reverse(unhex(arrayElement(head_words,i+1))))),
                    startsWith(param_types[i+1], 'int'),
                      toString(reinterpretAsInt256(reverse(unhex(arrayElement(head_words,i+1))))),
                    param_types[i+1] = 'bytes32',
                      concat('0x', arrayElement(head_words,i+1)),
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

      {% if output_json_type %}
        mapFromArrays(param_names, param_values_str) AS decoded_input
      {% else %}
        toJSONString(mapFromArrays(param_names, param_values_str)) AS decoded_input
      {% endif %}

    FROM tx_with_abi AS t
    ANY LEFT JOIN abi AS a
      ON substring(replaceAll(t.input,'0x',''),1,8) = a.selector
     AND t.abi_join_address = a.abi_contract_address
  )

SELECT
  block_number,
  block_timestamp,
  transaction_hash,
  contract_address,
  nonce,
  gas_price,
  value,
  {% if is_traces %}trace_address,{% endif %}
  function_name,
  decoded_input
FROM process
{% endset %}

{{ sql_body | trim }}
{% endmacro %}
