{{
    config(
        materialized='view',
        tags=["consensus", "validators", "graph_explorer"]
    )
}}

-- Thin projection that derives the controlling EVM address from a validator's
-- withdrawal_credentials. Saves downstream queries the substring() extraction.
-- Only rows with a 0x01-type credential expose a withdrawal_address; older
-- 0x00-type BLS credentials return NULL.

SELECT
    validator_index
    , pubkey
    , withdrawal_credentials
    , CASE
        WHEN startsWith(withdrawal_credentials, '0x01')
        THEN concat('0x', substring(withdrawal_credentials, 27, 40))
        ELSE NULL
      END AS withdrawal_address
FROM {{ ref('int_consensus_validators_labels') }}
