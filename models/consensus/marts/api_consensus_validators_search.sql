{{
    config(
        materialized='view',
        tags=["production", "consensus", 'tier1', 'api:validators_search', 'granularity:latest'],
        meta={
            "api": {
                "methods": ["GET", "POST"],
                "allow_unfiltered": true,
                "pagination": {
                    "enabled": true,
                    "default_limit": 5000,
                    "max_limit": 10000,
                    "response": "envelope"
                },
                "sort": [
                    {"column": "validator_count", "direction": "DESC"}
                ],
                "sortable_fields": ["validator_count", "withdrawal_credentials", "display_name"]
            }
        }
    )
}}

-- Dropdown source for the Validator Explorer tab.
--
-- Grain: one row per WITHDRAWAL_CREDENTIALS (not per validator). On Gnosis today this is
-- ~3,400 rows vs 558k at validator-grain, so the dashboard can load the full list in one
-- request (a few hundred KB over the wire) without 30s timeouts.
--
-- Collapsing to credential-grain matches the Explorer tab's semantics: every chart on
-- that tab aggregates across all validators sharing the selected withdrawal_credentials.
-- A solo validator is a "credential with validator_count = 1"; an operator is "credential
-- with validator_count = N".
--
-- LabelSelector filters client-side on substring match against every column the API
-- returns, so a user who pastes a withdrawal_credentials string (from a block explorer
-- or wallet app) will find the exact row. Pasting a validator_index or pubkey that isn't
-- in a `sample_validator_indexes`/`sample_pubkey` column won't match here — those lookups
-- need the richer /api/validators_performance_latest endpoint (filters by index / pubkey /
-- withdrawal_address) which stays at per-validator grain.
SELECT
    withdrawal_credentials
    ,min(validator_index) AS first_validator_index
    ,COUNT(*) AS validator_count
    ,any(withdrawal_address) AS withdrawal_address
    -- Display shape: "0x01…<last-10-of-address>  ·  N validators  (v#<first-index>)"
    -- Using the tail of the credential (the actual execution address) so operators with
    -- the same 0x01 type byte prefix are visually distinguishable in the dropdown.
    ,concat(
        substring(withdrawal_credentials, 1, 4)
        , '…'
        , substring(withdrawal_credentials, 57, 10)
        , ' · '
        , toString(COUNT(*))
        , ' validator'
        , if(COUNT(*) = 1, '', 's')
        , ' (v#'
        , toString(min(validator_index))
        , ')'
    ) AS display_name
FROM {{ ref('fct_consensus_validators_status_latest') }}
GROUP BY withdrawal_credentials
