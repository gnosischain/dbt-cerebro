{# ================================================================
   fetch_abi_from_blockscout.sql - Retrieve Contract ABI from Blockscout
   
   This macro retrieves a contracts ABI from the Blockscout API.
   
   Purpose:
   - Fetches the ABI JSON for a specified contract address
   - Makes an HTTP request to the Blockscout API
   - Extracts the ABI from the response
   - Returns the raw ABI as a JSON string
   
   Parameters:
   - contract_address: Ethereum address of the contract
   
   Returns:
   - ABI JSON string or empty array if not found
   
   Note:
   - This macro does not store the ABI - it only retrieves it
   - Usually called by fetch_and_insert_abi or other higher-level operations
   
   Usage Example:
   {% set abi_json = fetch_abi_from_blockscout('0xContractAddress') %}
================================================================ #}

{% macro fetch_abi_from_blockscout(contract_address) %}
    {% set blockscout_query %}
        WITH src AS (
            SELECT body 
            FROM url(
                'https://gnosis.blockscout.com/api/v2/smart-contracts/{{ contract_address | lower }}',
                'Raw',
                'body String'
            )
        )
        SELECT JSONExtractRaw(body, 'abi') AS abi_json
        FROM src
        LIMIT 1
    {% endset %}
    
    {% set abi_result = run_query(blockscout_query) %}
    
    {% if abi_result.rows | length > 0 %}
        {% set abi_json = abi_result[0][0] %}
        {% if abi_json and abi_json != '' %}
            {{ return(abi_json) }}
        {% else %}
            {{ log("No ABI found for contract " ~ contract_address, info=true) }}
            {{ return('[]') }}
        {% endif %}
    {% else %}
        {{ log("Failed to fetch ABI for contract " ~ contract_address, info=true) }}
        {{ return('[]') }}
    {% endif %}
{% endmacro %}