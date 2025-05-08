{# ================================================================
   fetch_and_insert_abi.sql - Fetch and Store Contract ABI
   
   This macro retrieves a contract ABI from Blockscout and stores it
   in the contracts_abi table.
   
   Purpose:
   - Calls fetch_abi_from_blockscout to get the ABI JSON
   - Escapes and formats the JSON for database storage
   - Inserts or replaces the ABI in the contracts_abi table
   - Provides logging for success/failure
   
   Parameters:
   - address: Ethereum address of the contract
   
   Requirements:
   - Requires contracts_abi table to exist with ReplacingMergeTree engine
   - Should be called via dbt operation:
     dbt run-operation fetch_and_insert_abi --args '{"address": "0xAddress"}'
   
   Note:
   - After running this macro, you should execute signature_generator.py
     to process the ABI into function and event signatures
   - Pair with decode_calls.sql and decode_logs.sql for full decoding
================================================================ #}

{% macro fetch_and_insert_abi(address) %}
    -- Fetch the initial contract details from blockscout
    {% set contract_data = fetch_abi_from_blockscout(address) %}
    {% set abi_json = contract_data['abi_json'] %}
    {% set contract_name = contract_data['contract_name'] %}
    {% set implementations_array = contract_data['implementations'] %}
    
    {{ log("Contract " ~ address ~ " has name: " ~ contract_name, info=true) }}
    {{ log("Contract " ~ address ~ " implementations array: " ~ implementations_array, info=true) }}
    
    -- First insert the contract ABI
    {% if abi_json and abi_json != '[]' and abi_json != '{}' %}
        -- Escape single quotes for ClickHouse VALUES syntax
        {% set abi_safe = abi_json | replace("'", "\\'") %}
        
        -- Check if this contract entry already exists
        {% set check_existing_sql %}
            SELECT count(*) as cnt
            FROM contracts_abi
            WHERE contract_address = '{{ address }}'
              AND implementation_address = ''
        {% endset %}
        
        {% set existing_result = run_query(check_existing_sql) %}
        {% set exists = existing_result.rows[0][0] > 0 %}
        
        {% if exists %}
            -- If exists, first delete the existing entry
            {% set delete_sql %}
                ALTER TABLE contracts_abi 
                DELETE WHERE contract_address = '{{ address }}' AND implementation_address = ''
            {% endset %}
            
            {{ log("Deleting existing ABI for " ~ address, info=true) }}
            {% do run_query(delete_sql) %}
        {% endif %}
        
        -- Insert the contract ABI
        {% set insert_sql %}
            INSERT INTO contracts_abi (
                contract_address, 
                implementation_address,
                abi_json, 
                contract_name,
                source, 
                updated_at
            )
            VALUES (
                '{{ address }}', 
                '',
                '{{ abi_safe }}',
                '{{ contract_name | replace("'", "\\'") }}',
                'blockscout',
                now()
            )
        {% endset %}

        {{ log("Inserting ABI for " ~ address, info=true) }}
        {% do run_query(insert_sql) %}
    {% else %}
        {{ log("No ABI to insert for " ~ address, info=true) }}
    {% endif %}
    
    -- Check if implementations array has content and appears to be an array
    {% set has_impls = implementations_array != '[]' and implementations_array != '' %}
    
    -- If this is a proxy contract with implementations
    {% if has_impls %}
        {{ log("Contract has implementations, extracting addresses using direct blockscout query", info=true) }}
        
        -- First run a command to enable JSON type support
        {% set enable_json_sql %}
            SET allow_experimental_json_type = 1
        {% endset %}
        
        {{ log("Enabling JSON type support", info=true) }}
        {% do run_query(enable_json_sql) %}
        
        -- Now extract implementation data in a separate query
        {% set extract_impl_query %}
            WITH src AS (
                SELECT body 
                FROM url(
                    'https://gnosis.blockscout.com/api/v2/smart-contracts/{{ address | lower }}',
                    'Raw',
                    'body String'
                )
            ),
            data AS (
                SELECT 
                    JSONExtractArrayRaw(body, 'implementations') AS implementations
                FROM src
            )
            SELECT
                JSONExtractString(implementations[1], 'address') AS impl_address,
                JSONExtractString(implementations[1], 'name') AS impl_name
            FROM data
        {% endset %}
        
        {{ log("Executing implementation extraction query", info=true) }}
        {% set impl_result = run_query(extract_impl_query) %}
        
        {% if impl_result.rows | length > 0 %}
            {% set impl_address = impl_result[0][0] %}
            {% set impl_name = impl_result[0][1] %}
            
            {{ log("Found implementation " ~ impl_address ~ " with name " ~ impl_name, info=true) }}
            
            -- Fetch the implementation contract ABI
            {% set impl_data = fetch_abi_from_blockscout(impl_address) %}
            {% set impl_abi_json = impl_data['abi_json'] %}
            
            {% if impl_abi_json and impl_abi_json != '[]' and impl_abi_json != '{}' %}
                -- Escape single quotes for ClickHouse VALUES syntax
                {% set impl_abi_safe = impl_abi_json | replace("'", "\\'") %}
                
                -- Check if this implementation entry already exists
                {% set check_impl_sql %}
                    SELECT count(*) as cnt
                    FROM contracts_abi
                    WHERE contract_address = '{{ address }}'
                      AND implementation_address = '{{ impl_address }}'
                {% endset %}
                
                {% set impl_existing_result = run_query(check_impl_sql) %}
                {% set impl_exists = impl_existing_result.rows[0][0] > 0 %}
                
                {% if impl_exists %}
                    -- If exists, first delete the existing entry
                    {% set delete_impl_sql %}
                        ALTER TABLE contracts_abi 
                        DELETE WHERE contract_address = '{{ address }}' AND implementation_address = '{{ impl_address }}'
                    {% endset %}
                    
                    {{ log("Deleting existing implementation ABI for " ~ address ~ " -> " ~ impl_address, info=true) }}
                    {% do run_query(delete_impl_sql) %}
                {% endif %}
                
                -- Insert the implementation ABI associated with the proxy
                {% set insert_impl_sql %}
                    INSERT INTO contracts_abi (
                        contract_address, 
                        implementation_address,
                        abi_json, 
                        contract_name,
                        source, 
                        updated_at
                    )
                    VALUES (
                        '{{ address }}', 
                        '{{ impl_address }}',
                        '{{ impl_abi_safe }}',
                        '{{ impl_name | replace("'", "\\'") }}',
                        'blockscout',
                        now()
                    )
                {% endset %}

                {{ log("Inserting implementation ABI for " ~ address ~ " -> " ~ impl_address, info=true) }}
                {% do run_query(insert_impl_sql) %}
                
                -- DO NOT insert the implementation as its own entry anymore
            {% else %}
                {{ log("No ABI available for implementation " ~ impl_address, info=true) }}
            {% endif %}
        {% else %}
            {{ log("No implementation found", info=true) }}
        {% endif %}
    {% else %}
        {{ log("Contract " ~ address ~ " has no implementations", info=true) }}
    {% endif %}
{% endmacro %}