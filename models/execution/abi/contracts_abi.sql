{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree()',
    order_by=['contract_address','implementation_address'],
    unique_key=['contract_address','implementation_address']
) }}

SELECT 
    '' AS contract_address,          -- The contract address (proxy or regular)
    '' AS implementation_address,    -- For proxy contracts, the implementation address; NULL for regular/implementation contracts
    '' AS abi_json,                  -- The ABI JSON
    '' AS contract_name,             -- Contract name from blockscout
    '' AS source,                    -- ABI source (e.g., 'blockscout')
    toDateTime('1970-01-01 00:00:00') AS updated_at  -- Last updated timestamp
WHERE 0=1
