{# ================================================================
   decode_calls.sql - Decode Ethereum Contract Function Calls
   
   This macro transforms raw transaction input data into readable parameters
   based on contract function signatures.
   
   Purpose:
   - Decodes transaction 'input' field using ABI function signatures
   - Identifies the function being called using the 4-byte selector
   - Extracts and converts parameters according to their types
   - Returns structured data with function name and decoded inputs
   
   Parameters:
   - tx_table: Source table containing transactions data
   - contract_address: Ethereum address of the contract to decode
   - output_json_type: Whether to return JSON object or string (default: false)
   - incremental_column: Column to use for incremental processing
   - selector_column: Column with the contract address to match (default: 'to_address')
   
   Requirements:
   - Requires function_signatures table with pre-generated signatures
   - Compatible with ClickHouse 24.1+ (no UDFs needed)
   
   Usage Example:
   {{ 
       decode_calls(
           tx_table = source('execution','transactions'),
           contract_address = '0xContractAddress',
           output_json_type = true
       )
   }}
================================================================ #}

{# ---------- helper: decode one 32‑byte ABI word ------------------ #}
{% macro _decode_word_call(hex_exp, type_exp) -%}
{% set t = "lower(trim(" ~ type_exp ~ "))" %}
multiIf(
    startsWith({{ t }}, 'uint256'),
        toString(reinterpretAsUInt256(reverse(unhex({{ hex_exp }})))),
    startsWith({{ t }}, 'int256'),
        toString(reinterpretAsInt256(reverse(unhex({{ hex_exp }})))),
    startsWith({{ t }}, 'address'),
        concat('0x', lower(hex(substring(unhex({{ hex_exp }}), 13, 20)))),
    startsWith({{ t }}, 'bool'),
        toString(reinterpretAsUInt8(unhex({{ hex_exp }})) != 0),
    startsWith({{ t }}, 'bytes32'),
        lower({{ hex_exp }}),
    NULL -- Default for unknown or unhandled types, or if hex_exp is NULL
)
{%- endmacro %}


{% macro decode_calls(
        tx_table,
        contract_address,
        output_json_type=false,
        incremental_column='block_timestamp',
        selector_column='to_address' 
) %}

{% set addr = contract_address | lower | replace('0x','') %}

{# ---------- 1. ABI rows (compile‑time) -------------------------- #}
{% set abi_sql %}
SELECT
    substring(signature,1,8) AS selector, -- First 4 bytes of keccak hash
    function_name,

    /* Sort input_params by 'position' to ensure correct argument order */
    arraySort(
        x -> toInt32OrZero(JSONExtractRaw(x, 'position')), -- Sort key
        JSONExtractArrayRaw(input_params)                   -- Array to sort
    ) AS sorted_inputs,

    /* Extract names and types from the sorted array */
    arrayMap(x -> JSONExtractString(x, 'name'), sorted_inputs) AS names,
    arrayMap(x -> JSONExtractString(x, 'type'), sorted_inputs) AS types
FROM {{ source('raw_abi','function_signatures') }}
WHERE replaceAll(lower(contract_address),'0x','') = '{{ addr }}'
{% endset %}


{# ---------- 2. runtime body ------------------------------------- #}
{% set sql_body %}
WITH
tx AS (
    SELECT *
    FROM {{ tx_table }}
    WHERE replaceAll(lower({{ selector_column }}),'0x','') = '{{ addr }}'
      {% if incremental_column and not flags.FULL_REFRESH %}
        AND {{ incremental_column }} >
            (SELECT coalesce(max({{ incremental_column }}), '1970-01-01')
             FROM {{ this }})
      {% endif %}
),
abi AS ( {{ abi_sql }} ),

dec AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash, 
        t.nonce,
        t.gas_price,
        t.value_string AS value,

        -- Extract selector from transaction input
        substring(replaceAll(t.input, '0x', ''), 1, 8) AS call_selector_from_input,
        a.function_name,

        -- Prepare argument data (hex string *after* selector)
        substring(replaceAll(t.input, '0x', ''), 9) AS args_raw_hex,

        -- Pad the raw argument hex to be a multiple of 64 characters
        lpad(
            args_raw_hex,
            intDiv(length(args_raw_hex) + 63, 64) * 64,
            '0'
        ) AS args_padded_hex,

        -- Split padded arguments into 32-byte (64-char) words
        intDiv(length(args_padded_hex), 64) AS num_arg_words_present_in_data,
        arrayMap(w -> substring(args_padded_hex, 1 + 64 * w, 64),
                 range(num_arg_words_present_in_data)) AS ARG_WORDS_ARRAY,

        -- ABI information for arguments
        a.names AS N,
        a.types AS T,
        length(T) AS arg_count_from_abi, -- Number of arguments defined in ABI

        -- Decode each argument based on ABI
        -- Iterates 'i' from 0 to (arg_count_from_abi - 1)
        arrayMap(i ->
            {{ _decode_word_call(
                "if(i + 1 > num_arg_words_present_in_data, NULL, arrayElement(ARG_WORDS_ARRAY, i + 1))",
                "arrayElement(T, i + 1)"
            ) }},
            range(arg_count_from_abi) -- Iterate for each argument defined in the ABI
        ) AS VALUES,

        -- Construct JSON output
        {% if output_json_type %}
            mapFromArrays(N, VALUES) AS decoded_input
        {% else %}
            toJSONString(mapFromArrays(N, VALUES)) AS decoded_input
        {% endif %}
    FROM tx AS t
    ANY LEFT JOIN abi AS a
        ON call_selector_from_input = a.selector -- Join on the extracted selector
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
FROM dec
{% endset %}

{{ sql_body | trim }}
{% endmacro %}