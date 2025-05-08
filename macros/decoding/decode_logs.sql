{# ================================================================
   decode_logs.sql - Decode Ethereum Contract Event Logs
   
   This macro transforms raw blockchain event logs into readable parameters
   based on contract event signatures.
   
   Purpose:
   - Decodes event logs using ABI event signatures
   - Identifies events by their topic0 (event signature hash)
   - Processes both indexed parameters (topics) and non-indexed (data)
   - Returns structured data with event name and decoded parameters
   
   Parameters:
   - source_table: Source table containing the event logs
   - contract_address: Ethereum address of the contract to decode
   - output_json_type: Whether to return JSON object or string (default: false)
   - incremental_column: Column to use for incremental processing
   - event_signature: Optional specific event signature to decode
   
   Requirements:
   - Requires event_signatures table with pre-generated signatures  
   - Compatible with ClickHouse 24.1+ (no UDFs needed)
   
   Usage Example:
   {{ 
       decode_logs(
           source_table = source('execution','logs'),
           contract_address = '0xContractAddress',
           output_json_type = true
       )
   }}
================================================================ #}

{%- macro _decode_word(hex_exp, type_exp) -%}
    /* 32‑byte ABI word → String */
    multiIf(
        {{ type_exp }} = 'uint256',
            toString(reinterpretAsUInt256(reverse(unhex({{ hex_exp }})))),
        {{ type_exp }} = 'int256',
            toString(reinterpretAsInt256(reverse(unhex({{ hex_exp }})))),
        {{ type_exp }} = 'address',
            concat('0x', lower(hex(substring(unhex({{ hex_exp }}), 13, 20)))),
        {{ type_exp }} = 'bool',
            toString(reinterpretAsUInt8(unhex({{ hex_exp }})) != 0),
        {{ type_exp }} = 'bytes32',
            lower({{ hex_exp }}),
        {{ type_exp }} = 'bytes',
            lower({{ hex_exp }}),
        NULL
    )
{%- endmacro %}

{% macro decode_logs(
        source_table,
        contract_address,
        output_json_type=false,         
        incremental_column='block_timestamp',
        event_signature=None           
) %}

{# -------- 1. normalise address ---------- #}
{% set addr = contract_address | lower | replace('0x','') %}

{# -------- 2. ABI rows (compile‑time) ---- #}
{% set sig_sql %}
SELECT
    replace(signature,'0x','')                        AS topic0_sig,
    event_name,
    arrayMap(x->JSONExtractString(x,'name'),
             JSONExtractArrayRaw(params))             AS names,
    arrayMap(x->JSONExtractString(x,'type'),
             JSONExtractArrayRaw(params))             AS types,
    arrayMap(x->JSONExtractBool(x,'indexed'),
             JSONExtractArrayRaw(params))             AS flags
FROM {{ source('raw_abi','event_signatures') }}
WHERE replaceAll(lower(contract_address),'0x','') = '{{ addr }}'
{% if event_signature %}
  AND signature = replace('{{ event_signature }}','0x','')
{% endif %}
{% endset %}

{# -------- 3. runtime body --------------- #}
{% set sql_body %}
WITH
logs AS (
    SELECT *
    FROM {{ source_table }}
    WHERE replaceAll(lower(address),'0x','') = '{{ addr }}'
      {% if incremental_column and not flags.FULL_REFRESH %}
        AND {{ incremental_column }} >
            (SELECT coalesce(max({{ incremental_column }}), '1970‑01‑01')
             FROM {{ this }})
      {% endif %}
),
abi AS ( {{ sig_sql }} ),

dec AS (
    SELECT
        l.block_number, l.block_timestamp,
        l.transaction_hash, l.transaction_index,
        l.log_index,      l.address AS contract_address,
        toStartOfMonth(l.block_timestamp) AS partition_month,
        a.event_name,

        /* arrays from ABI */
        a.names  AS N,
        a.types  AS T,
        a.flags  AS F,
        range(length(T)) AS I,
        arrayZip(F,T,I)  AS Z,

        /* indexed params (topics) */
        arrayFilter(z -> z.1 = 1, Z) AS ZI,
        arrayMap(z ->
            {{ _decode_word(
                "replaceAll(arrayElement([l.topic1,l.topic2,l.topic3], z.3 + 1),'0x','')",
                "z.2") }},
            ZI) AS VI,

        /* non‑indexed params (data) */
        replaceAll(l.data,'0x','')                          AS data_hex,
        intDiv(length(data_hex), 64)                        AS words_n,
        arrayMap(w -> substring(data_hex,1+64*w,64),range(words_n)) AS WORDS,
        arrayFilter(z -> z.1 = 0, Z)                        AS ZD,
        arrayMap(j ->
            {{ _decode_word("WORDS[j+1]", "arrayElement(ZD,j+1).2") }},
            range(length(ZD)))                              AS VD,

        /* stitch back ABI order */
        arrayMap(k ->
            if(F[k+1]=1,
               VI[arrayReduce('sum', arraySlice(F,1,k+1))],
               VD[k+1 - arrayReduce('sum', arraySlice(F,1,k+1))]
            ), I)                                           AS VALUES,

        {% if output_json_type %}
            mapFromArrays(N, VALUES)                       AS decoded_params
        {% else %}
            toJSONString(mapFromArrays(N, VALUES))          AS decoded_params
        {% endif %}
    FROM logs AS l
    ANY LEFT JOIN abi AS a
        ON replaceAll(l.topic0,'0x','') = a.topic0_sig
)

SELECT
    block_number
    ,block_timestamp
    ,transaction_hash
    ,transaction_index
    ,log_index
    ,contract_address
    ,event_name
    ,decoded_params
FROM dec
{% endset %}

{% if output_json_type %}
    {{ sql_body | trim }}
{% else %}
    {{ sql_body | trim }}
{% endif %}
{% endmacro %}
