{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(validator_index)',
        tags=["production", "consensus", "validators"]
    )
}}

SELECT 
    validator_index
    ,pubkey
    ,withdrawal_credentials
FROM stg_consensus__validators
WHERE slot = (SELECT MAX(slot) FROM stg_consensus__validators)