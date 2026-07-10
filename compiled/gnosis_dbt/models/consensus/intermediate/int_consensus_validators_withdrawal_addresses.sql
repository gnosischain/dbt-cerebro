

-- Thin projection that derives the controlling EVM address from a validator's
-- withdrawal_credentials. Saves downstream queries the substring() extraction.
-- 0x01 (eth1) and 0x02 (EIP-7251 compounding) credentials both encode the
-- 20-byte execution address at the same byte offset (confirmed against the
-- consensus-specs Electra beacon-chain.md: has_execution_withdrawal_credential
-- covers both prefixes, and every address-extraction call site uses the
-- identical withdrawal_credentials[12:] slice regardless of prefix). Only
-- 0x00-type BLS credentials (no execution address) return NULL.

SELECT
    validator_index
    , pubkey
    , withdrawal_credentials
    , CASE
        WHEN startsWith(withdrawal_credentials, '0x01')
          OR startsWith(withdrawal_credentials, '0x02')
        THEN concat('0x', substring(withdrawal_credentials, 27, 40))
        ELSE NULL
      END AS withdrawal_address
FROM `dbt`.`int_consensus_validators_labels`