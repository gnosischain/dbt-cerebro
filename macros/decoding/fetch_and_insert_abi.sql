{# ================================================================
   fetch_and_insert_abi.sql - Fetch and Store Contract ABI
   
   This macro retrieves a contract ABI from Blockscout and stores it
   in the contract_abis table.
   
   Purpose:
   - Calls fetch_abi_from_blockscout to get the ABI JSON
   - Escapes and formats the JSON for database storage
   - Inserts or replaces the ABI in the contract_abis table
   - Provides logging for success/failure
   
   Parameters:
   - address: Ethereum address of the contract
   
   Requirements:
   - Requires contract_abis table to exist with ReplacingMergeTree engine
   - Should be called via dbt operation:
     dbt run-operation fetch_and_insert_abi --args '{"address": "0xAddress"}'
   
   Note:
   - After running this macro, you should execute signature_generator.py
     to process the ABI into function and event signatures
   - Pair with decode_calls.sql and decode_logs.sql for full decoding
================================================================ #}

{% macro fetch_and_insert_abi(address) %}
    {% set abi_json = fetch_abi_from_blockscout(address) %}

    {# Only insert if we actually got something back #}
    {% if abi_json and abi_json != '[]' %}

        {# escape single quotes for ClickHouse VALUES syntax #}
        {% set abi_safe = abi_json | replace("'", "\\'") %}

        {# First check if this contract already exists #}
        {% set check_existing_sql %}
            SELECT count(*) as cnt
            FROM contract_abis
            WHERE contract_address = '{{ address }}'
        {% endset %}
        
        {% set existing_result = run_query(check_existing_sql) %}
        {% set exists = existing_result.rows[0][0] > 0 %}
        
        {% if exists %}
            {# If exists, first delete the existing entry #}
            {% set delete_sql %}
                ALTER TABLE contract_abis 
                DELETE WHERE contract_address = '{{ address }}'
            {% endset %}
            
            {{ log("Deleting existing ABI for " ~ address, info=true) }}
            {% do run_query(delete_sql) %}
        {% endif %}
        
        {# Now insert the new or updated ABI #}
        {% set insert_sql %}
            INSERT INTO contract_abis (contract_address, abi_json, source, updated_at)
            VALUES (
                '{{ address }}', 
                '{{ abi_safe }}',
                'blockscout',
                now()
            )
        {% endset %}

        {{ log("Inserting ABI for " ~ address, info=true) }}
        {% do run_query(insert_sql) %}
        
    {% else %}
        {{ log("Nothing to insert for " ~ address, info=true) }}
    {% endif %}
{% endmacro %}