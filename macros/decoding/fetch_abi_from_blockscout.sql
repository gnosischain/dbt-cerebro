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
        SELECT 
            JSONExtractRaw(body, 'abi') AS abi_json,
            JSONExtractString(body, 'name') AS contract_name,
            JSONHas(body, 'implementations') AS has_implementations,
            JSONExtractArrayRaw(body, 'implementations') AS implementations
        FROM src
        LIMIT 1
    {% endset %}
    
    {% set abi_result = run_query(blockscout_query) %}
    
    {% if abi_result.rows | length > 0 %}
        {% set result = {
            'abi_json': abi_result[0][0],
            'contract_name': abi_result[0][1],
            'has_implementations': abi_result[0][2],
            'implementations': abi_result[0][3]
        } %}
        
        {{ return(result) }}
    {% else %}
        {{ log("Failed to fetch ABI for contract " ~ contract_address, info=true) }}
        {{ return({'abi_json': '[]', 'contract_name': '', 'has_implementations': 0, 'implementations': '[]'}) }}
    {% endif %}
{% endmacro %}